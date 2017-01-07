package transaction

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import collection.mutable.HashMap 
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

sealed trait TransactionAppAPI
case class Balance(applicationID:Int,transactionID:Int) extends TransactionAppAPI
case class Deposit(applicationID:Int,transactionID:Int,value:Int) extends TransactionAppAPI
case class Withdraw(applicationID:Int,transactionID:Int,value:Int) extends TransactionAppAPI
case class Transfer(applicationID:Int,transactionID:Int,target:Int,value:Int) extends TransactionAppAPI
case class Begin_Transaction(applicationID:Int, transactionID:Int) extends TransactionAppAPI
case class End_Transaction(applicationID:Int, transactionID:Int) extends TransactionAppAPI


class TransactionApp(val applicationID:Int,val clientId: ActorRef) extends Actor{
	var transactionID = 1

	def receive() = {
		case Result(result) =>
			output(result)
		case App_check() =>
			app_check()
		case App_deposit(value) =>
			app_deposit(value)
		case App_withdraw(value) =>
			app_withdraw(value)
		case App_transfer(target,value) =>
			app_transfer(target,value)
		case App_Begin_transaction() =>
			app_begin_transaction()
		case App_End_transaction() =>
			app_end_transaction()
		case Commit(transactionID) =>
			println(applicationID.toString + " : " + transactionID.toString + " commit")
		case Abort(transactionID) =>
			println(applicationID.toString + " : " + transactionID.toString + " abort")

	}

	private def app_begin_transaction() = 
	{
		clientId ! Begin_Transaction(applicationID,transactionID)

	}

	private def app_end_transaction() = 
	{
		clientId ! End_Transaction(applicationID,transactionID)
		transactionID += 1
	}

	private def app_check()
	{
		clientId ! Balance(applicationID,transactionID)
		//transactionID = transactionID + 1
	}

	private def app_deposit(value:Int)
	{
		clientId ! Deposit(applicationID,transactionID,value)
		//transactionID = transactionID + 1
	}

	private def app_withdraw(value:Int)
	{
		clientId ! Withdraw(applicationID,transactionID,value)
		//transactionID = transactionID + 1
	}

	private def output(result: String)
	{
		println(applicationID.toString+": "+result)
	}

	private def app_transfer(target: Int, value:Int)
	{
		clientId ! Transfer(applicationID,transactionID,target,value)
		//transactionID = transactionID + 1
	}

}

object TransactionApp{
	def props(applicationID: Int,clientId:ActorRef):Props = {
		Props(classOf[TransactionApp],applicationID,clientId)
	}
}