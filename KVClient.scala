package transaction

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import collection.mutable.HashMap 
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

class Client_Operation_Obj(Opname:String,SourceID:Int, TargetID:Int, Value: Int)
{
	var opname = Opname
	var sourceID = SourceID
	var targetID = TargetID
	var value = Value 
}

//case class Release(key: Int,clientID:Int) extends TransactionClientAPI

sealed trait ResultAPI
case class Result(Output: String) extends ResultAPI
case class Abort(transactionID:Int) extends ResultAPI
case class Commit(transactionID:Int) extends ResultAPI

case class Clear_cache(Key:Int) extends ResultAPI

class TransactionClient(val system:ActorSystem,val clientID: Int,val server: Seq[ActorRef]) extends Actor{

	private var lock_type = new HashMap[Int,Int] () {override def default (key:Int) = 0}
	private var balance = new HashMap[Int,Int] () {override def default (key:Int) = -1}
	private var version = new HashMap[Int,Int] () {override def default (key:Int) = 0}
	private var FAILED = -2
	private var N = 5
	implicit val timeout = Timeout(1000 seconds)
	var endpoints: Option[Seq[ActorRef]] = None
	var clients:Option[Seq[ActorRef]] = None
	
	var oplist_map = new HashMap[Int,ListBuffer[Client_Operation_Obj]] //key: transaction ID, value: operations

	def receive() = {
		case Balance(applicationID,transactionID) =>
			oplist_map(applicationID*1000+transactionID) += new Client_Operation_Obj("Balance",0,applicationID,0)
		case Deposit(applicationID,transactionID,value) => 
			oplist_map(applicationID*1000+transactionID) += new Client_Operation_Obj("Deposit",0,applicationID,value)
		case Withdraw(applicationID,transactionID,value) => 
			oplist_map(applicationID*1000+transactionID) += new Client_Operation_Obj("Withdraw",0,applicationID,value)
		case Transfer(applicationID,transactionID,target,value) =>
			oplist_map(applicationID*1000+transactionID) += new Client_Operation_Obj("Transfer",applicationID,target,value)
		case Clear_cache(key) =>
			clear_cache(key)
		case Begin_Transaction(applicationID,transactionID) =>
			begin_transaction(applicationID,transactionID)
		case End_Transaction(applicationID,transactionID) =>
			end_transaction(applicationID,transactionID)
		case View(e) =>
			endpoints = Some(e)
		case Viewclients(e) =>
			clients = Some(e)
	}

	private def begin_transaction(applicationID:Int,transactionID:Int) = 
	{
		oplist_map += (applicationID*1000+transactionID -> new ListBuffer[Client_Operation_Obj])
	}

	private def end_transaction(applicationID:Int,transactionID:Int) = 
	{
		//create a lock list
		var lock_list = new HashMap[Int,ListBuffer[Lock_Obj]]()
		var store_set = Set[Int]()
		for(op <- oplist_map(applicationID*1000+transactionID))
		{
			var store = route(applicationID)
			store_set += store
			if(!lock_list.contains(store))
				lock_list += (store->ListBuffer[Lock_Obj]())
			var lock1 = new Lock_Obj(applicationID,'w')
			var lock2 = new Lock_Obj(op.targetID,'w')
			if(op.opname == "Balance")
				lock1.rw = 'r'

			if(op.opname == "Transfer")
			{
				lock_list(store) += lock1
				var store2 = route(op.targetID)
				if(!lock_list.contains(store2))
					lock_list += (store2->ListBuffer[Lock_Obj]())
				lock_list(store2) += lock2
				store_set += store2
			}
			else
				lock_list(store) += lock1
		}
		var commit = true;
		//acquire locks from each server

		for(id <- store_set)
		{
			var flag = false
			for(i <- 0 until N)
			{
				if(flag == false)
				{
					val future = server(id*N+i) ? Primary_Grap_Locks(lock_list(id),applicationID*1000+transactionID)
					var result = Await.result(future,timeout.duration).asInstanceOf[Int]
					if(result != FAILED)
					{
						if(result == 0)
							commit = false
						flag = true
					}
				}
			}
		}
		/*
		for(id <- store_set)
		{
			var flag = false
			for(i <- 0 until N)
			{
				val future = server(id*N+i) ? Primary_Grap_Locks(lock_list(id),applicationID*1000+transactionID)
				var result = Await.result(future,timeout.duration).asInstanceOf[Int]
				if(result != FAILED)
				{
					if(flag == false)
					{
						if(result != FAILED)
						{
							if(result == 0)
								commit = false
							flag = true
						}
					}
				}
			}
		}*/
		//send operations to each server

		//initiate commit
		//decide to abort or commit 

		//send comit or abort 
		
		if(commit == false)
		{
			for(id <- store_set)
			{
				for(i <- 0 until N)
				{
					server(id*N+i) ! Abort(applicationID*1000+transactionID)
				}
			}
		}
		else
		{	
			for(op <- oplist_map(applicationID*1000+transactionID))
			{
				var store = route(applicationID)
				
				if(op.opname == "Balance")
				{
					check_balance(applicationID,transactionID,store)
				}
				else if(op.opname == "Deposit")
				{
					deposit_money(applicationID,transactionID,op.value,store)
				}
				else if(op.opname == "Withdraw")
				{
					withdraw_money(applicationID,transactionID,op.value,store)
				}
				else if(op.opname == "Transfer")
				{
					var store2 = route(op.targetID)
					transfer_money(applicationID,transactionID,op.targetID,op.value,store,store2)
				}
			}
		}

		var final_commit = commit
		for(id <- store_set)
		{
			for(i <- 0 until N)
			{
				val future = server(id*N+i) ? Init_commit(applicationID*1000+transactionID)
				var result = Await.result(future,timeout.duration).asInstanceOf[Any]
				if(result != FAILED)
				{
					if(result == None)
						final_commit = false
				}

			}
		}
		
		if(final_commit)
		{
			for(id <- store_set)
			{
				for(i <- 0 until N)
				{
					server(id*N+i) ! Commit_store(applicationID*1000+transactionID)
				}
			}
		}
		else
		{
			for(id <- store_set)
			{
				for(i <- 0 until 1)
				{
					server(id*N+i) ! Abort_store(applicationID*1000+transactionID)
				}
			}
		
		}
		//return 'commit or abort' to application
		
		if(final_commit)
			endpoints.get(applicationID) ! Commit(transactionID)
		else
			endpoints.get(applicationID) ! Abort(transactionID)
		//commit
		
	}

	private def implement_read(server_id : Int,applicationID: Int, transactionID: Int) : Int = {
		var max_version = 0
		var value = 0
		for(i <- 0 until N)
		{
			var future = server(server_id*N+i) ? Get(applicationID,applicationID * 1000 + transactionID)
			var result = Await.result(future,timeout.duration).asInstanceOf[Any]
			if(result != FAILED)
			{
				if(result != None)
				{
					var unit = result.asInstanceOf[Array[Int]]
					if(unit(1) > 0)
					{
						if(unit(1) > max_version)
						{
							value = unit(0)
							max_version = unit(1)
						}
						if(unit(1) < max_version)
						{
							recover(server_id,i)
						}
					}
				}
				else
				{
					if(0 >= max_version)
						value = 0
				}
			}
			else
			{
				println("replica "+i+" failed")
			}
		}
		version(applicationID) = max_version
		value
	}

	private def implement_write(server_id : Int, applicationID: Int, transactionID: Int,cur_balance: Int)
	{
		var recover_set = Set(5)
		for(i <- 0 until N)
		{
			val future = server(server_id*N+i) ? Put(applicationID,cur_balance,applicationID*1000+transactionID,version(applicationID)+1)
			var result = Await.result(future,timeout.duration).asInstanceOf[Int]
			if(result != FAILED)
			{
				if(result == 0) // stale
				{
					//recover(server_id,i)
					recover_set += i
				}
			}
			else
			{
				println("replica "+i+" failed")
			}
		}
		for(num <- recover_set)
		{
			if(num != 5)
			{
				recover(server_id,num)
			}
		}
		version(applicationID) = version(applicationID)+1
	}

	private def recover(server_id: Int, number: Int)
	{
		var max_version = -1
		var newest = 0
		for(i <- 0 until N)
		{
			if(i != number)
			{
				val future = server(server_id*N+i) ? Get_Version()
				val result = Await.result(future,timeout.duration).asInstanceOf[Int]
				if(result != FAILED)
				{
					if(result > max_version)
					{
						max_version = result
						newest = i
					}
				}
			}
		}
		copy(server_id,newest,number)
	}

	private def copy(server_id: Int, from: Int, to:Int)
	{
		val future = server(server_id*N+from) ? Get_Value()
		val result = Await.result(future,timeout.duration).asInstanceOf[scala.collection.mutable.HashMap[Int, Any]]
		if(result != FAILED)
		{
			server(server_id*N+to) ! Put_Value(result)
		}
		val future1 = server(server_id*N+from) ? Get_Version_Map()
		val result1 = Await.result(future1,timeout.duration).asInstanceOf[scala.collection.mutable.HashMap[Int,Int]]
		if(result != FAILED)
		{
			server(server_id*N+to) ! Put_Version_Map(result1)
		}
	}

	private def route(key: Int): Int = {
    	(key % (server.length / N)).toInt
  	}

	private def check_balance(applicationID: Int,transactionID: Int,store: Int)
	{
		var cur_balance = balance(applicationID)
		if(cur_balance == -1)
		{
			cur_balance = implement_read(store,applicationID,applicationID*1000+transactionID)
		}
		endpoints.get(applicationID) ! Result(transactionID.toString+" done, balance:"+cur_balance.toString)

	}

	private def deposit_money(applicationID: Int,transactionID: Int,value: Int,store: Int)
	{
		
		var cur_balance = balance(applicationID)
		if(cur_balance == -1)
		{
			cur_balance = implement_read(store,applicationID,applicationID*1000+transactionID)
		}
		cur_balance = cur_balance+value
		implement_write(store,applicationID,applicationID*1000+transactionID,cur_balance)
		balance(applicationID) = cur_balance
		invalidate(applicationID)
		endpoints.get(applicationID) ! Result(transactionID.toString+" done, balance:"+cur_balance.toString)	
		
	}

	private def withdraw_money(applicationID:Int,transactionID:Int,value:Int,store:Int)
	{
		
		var cur_balance = balance(applicationID)
		if(cur_balance == -1)
		{
			cur_balance = implement_read(store,applicationID,applicationID*1000+transactionID)
		}
		cur_balance = cur_balance - value
		if(cur_balance < 0)
		{
			endpoints.get(applicationID) ! Result(transactionID.toString+" failed, not sufficient balance")
		}
		else
		{
			implement_write(store,applicationID,applicationID*1000+transactionID,cur_balance)
			balance(applicationID) = cur_balance
			invalidate(applicationID)
			endpoints.get(applicationID) ! Result(transactionID.toString+" done, balance:"+cur_balance.toString)
		}
	}

	private def transfer_money(applicationID: Int,transactionID: Int,target:Int,value:Int,store: Int,store2: Int)
	{
		
		var cur_balance1 = balance(applicationID)
		if(cur_balance1 == -1)
		{
			cur_balance1 = implement_read(store,applicationID,applicationID*1000+transactionID)
		}


		var cur_balance2 = balance(target)

		if(cur_balance2 == -1)
		{
			cur_balance2 = implement_read(store2,target,applicationID*1000+transactionID)
		}

		cur_balance1 = cur_balance1 - value
		cur_balance2 = value + cur_balance2
		if(cur_balance1 < 0)
		{
			endpoints.get(applicationID) ! Result(transactionID.toString+" failed, not sufficient balance")
		}
		else
		{
			implement_write(store,applicationID,applicationID*1000+transactionID,cur_balance1)
			balance(applicationID) = cur_balance1
			invalidate(applicationID)
			implement_write(store2,target,applicationID*1000+transactionID,cur_balance2)
			balance(target) = cur_balance2
			invalidate(target)
			endpoints.get(applicationID) ! Result(transactionID.toString+" done, balance:"+cur_balance1.toString)
		}
	}



	private def invalidate(key:Int)
	{
		for(i <- 0 until 10)
		{
			if(i != clientID)
				clients.get(i) ! Clear_cache(key)
		}
	}
	private def clear_cache(key:Int)
	{
		balance(key) = -1
	}
}
object TransactionClient{
	def props(system:ActorSystem,clientID:Int,server:Seq[ActorRef]):Props = {
		Props(classOf[TransactionClient],system,clientID,server)
	}
}