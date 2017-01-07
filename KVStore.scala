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

class Lock_Obj(var key_in:Int, var rw_in: Char)
{
  var key = key_in
  var rw = rw_in
}

class Lock_status()
{
  var reader_list:ListBuffer[Int] = new ListBuffer[Int]()
  var writer_list:Boolean = false
}

class Operator_Obj(var key_in:Int,var value_in:Any)
{
  var key:Int = key_in
  //var rw:Char = rw_in
  var value:Any = value_in
}

//class Ops_Log()
//{
 // var Marker = 0
 // var Log_Table = new List[Operator_Obj]]
//}


sealed trait KVStoreAPI
case class Put(key: Int, value: Any, Tid: Int,version: Int) extends KVStoreAPI
case class Get(key: Int, Tid: Int) extends KVStoreAPI
case class Grap_Locks(locks:ListBuffer[Lock_Obj], Tid: Int) extends KVStoreAPI
case class Init_commit(Tid:Int) extends KVStoreAPI
case class Abort_store(Tid:Int) extends KVStoreAPI
case class Commit_store(Tid: Int) extends KVStoreAPI
case class Primary_Grap_Locks(locks:ListBuffer[Lock_Obj],Tid:Int) extends KVStoreAPI
case class Update_lock_status(lmap:HashMap[Int,Lock_status]) extends KVStoreAPI
case class Update_lock_list(lmap:HashMap[Int,ListBuffer[Lock_Obj]]) extends KVStoreAPI
case class Update_Rollback(lmap:HashMap[Int,HashMap[Int,Any]]) extends KVStoreAPI
case class Get_Version() extends KVStoreAPI
case class Get_Value() extends KVStoreAPI 
case class Get_Version_Map() extends KVStoreAPI
case class Put_Version_Map(m:HashMap[Int,Int]) extends KVStoreAPI 
case class Put_Value(m:HashMap[Int, Any]) extends KVStoreAPI 
case class Disconnect_replica() extends KVStoreAPI
case class Connect_replica() extends KVStoreAPI

//case class Viewreplicas(replicas:Array[ActorRef]) extends KVStoreAPI

/**
 * KVStore is a local key-value store based on actors.  Each store actor controls a portion of
 * the key space and maintains a hash of values for the keys in its portion.  The keys are 128 bits
 * (Int), and the values are of type Any.
 */

class KVStore extends Actor {
  private var store = new HashMap[Int, Any] {override def default(key:Int) = 0 }
  private var lockmap = new HashMap[Int,Lock_status] //1: free 2: read 3: write
  private var operation_Log = new HashMap[Int,ListBuffer[Operator_Obj]]
  private var locklist_map = new HashMap[Int,ListBuffer[Lock_Obj]]
  private var rollback_value = new HashMap[Int,HashMap[Int,Any]]
  private var timeout_map = new HashMap[Int,Int]
  implicit val timeout = Timeout(1000 seconds)
  private var disconnected = false
  private var FAILED = -2
  private var is_primary = false
  //version number of each key,
  private var version_map = new HashMap[Int,Int] () {override def default (key:Int) = 0}
  private var ID = 0
  //replicas of the servers
  private var replicas: Option[Seq[ActorRef]] = None

  override def receive = {
    case Put(key,cell,tid,version) =>
      //sender ! store.put(key,cell)
    if(disconnected)
      sender ! FAILED
    else
      handle_put(key,cell,tid,version)
    case Get(key,tid) =>
    if(disconnected)
      sender ! FAILED
    else
      handle_get(key,tid)
      //sender ! store.get(key)
    case Grap_Locks(locks,tid) =>
    if(disconnected)
      sender ! FAILED
    else 
      handle_grap_locks(locks,tid)
    case Init_commit(tid) =>
    if(disconnected)
      sender ! FAILED
    else
      handle_init_commit(tid)
    case Abort_store(tid) =>
    if(disconnected == false)
      handle_abort(tid)
    case Commit_store(tid) =>
    if(disconnected == false)
      handle_commit(tid)
    case Viewreplicas(r) =>
    replicas = Some(r)
    case Update_lock_status(lmap) =>

    for ((key,value) <- lmap)
    {
      lockmap(key) = value
    }
    //lockmap = lmap
    sender ! 1
    case Update_lock_list(list_map) =>
    for ((key,value) <- list_map)
    {
      locklist_map(key) = value
    }
    //locklist_map = list_map
    sender ! 1
    case Update_Rollback(rollback) =>
    for((key,value) <- rollback)
    {
      rollback_value(key) = value
    }
    sender ! 1
    case Primary_Grap_Locks(locks:ListBuffer[Lock_Obj], tid: Int) =>
    if(disconnected)
      sender ! FAILED
    else
    {
      is_primary = true
      handle_primary_grap_locks(locks,tid)
    }
    case Get_Version() =>
    if(disconnected)
      sender ! FAILED
    else
    {
      var sum:Int = 0

      for((key,value) <- version_map)
      {
        sum += value
      }
      sender ! sum
    }
    case Get_Value() =>
    if(disconnected)
      sender ! FAILED
    else
      sender ! store

    case Put_Value(m) =>
    //store = m.asInstanceOf[scala.collection.mutable.HashMap[Int, Any] {def default(key:Int):Int}]
    for ((key,value)<-m)
    {
      store(key) = value
    }
    case Get_Version_Map() =>
    if(disconnected)
      sender ! FAILED
    else
      sender ! version_map
    case Put_Version_Map(m) =>
    for((key,value) <- m)
    {
      version_map(key) = value
    }
    case Disconnect_replica() =>
      disconnected = true
    case Connect_replica() =>
      disconnected = false
    case ViewID(id) =>
      ID = id


  }




def handle_init_commit(Tid:Int) =
{

  if(operation_Log.contains(Tid))
  {
    var list = operation_Log(Tid)
    for (op <- list)
    {
      store(op.key) = op.value
    }
    operation_Log -= Tid
  }
  if(disconnected)
    sender ! FAILED
  else
    sender ! 1 // 1
}

def handle_put(key:Int,cell:Any,Tid:Int,version:Int) =
{
  if(!operation_Log.contains(Tid))
  {
    operation_Log += (Tid->new ListBuffer[Operator_Obj]())
  }
  if(version_map(key) +1 == version)
  {
    store(key) = cell
    var operator = new Operator_Obj(key,cell)
    operation_Log(Tid) += operator
    version_map(key) = version
    if(disconnected)
      sender ! FAILED
    else
      sender ! 1
  }
  else
  {
    if(disconnected)
      sender ! FAILED
    else
      sender ! 0
  }
  //Transaction_Log
}

def handle_get(key:Int,Tid:Int) =
{
  if(operation_Log.contains(Tid))
  {
      val list = operation_Log(Tid)
      var flag = false;
      for(op <- list.reverse)
      {
        if(op.key == key && !flag)
        {
          flag = true
          var unit: Array[Int] = Array(op.value.asInstanceOf[Int],version_map(key))
          if(disconnected)
            sender ! FAILED
          else
            sender ! unit
        }
      }
      if(flag == false)
      {
        if(store.get(key) == None)
        {
          if(disconnected)
            sender ! FAILED
          else
            sender ! store.get(key)
        }
        else
        {
          var unit: Array[Int] = Array(store.get(key).get.asInstanceOf[Int],version_map(key))
          if(disconnected)
            sender ! FAILED
          else
            sender ! unit
        }
      }
  }
  else
  {
      if(store.get(key) == None)
      { 
        if(disconnected)
          sender ! FAILED
        else
          sender ! store.get(key)
      }
      else
      { 
        var unit: Array[Int] = Array(store.get(key).get.asInstanceOf[Int],version_map(key))
        if(disconnected)
          sender ! FAILED
        else
          sender ! unit
      }
  }
  //return the most recent value in the log-append file..


}

def handle_primary_grap_locks(locks:ListBuffer[Lock_Obj],Tid:Int)
{
  rollback_value += (Tid->new HashMap[Int,Any])
  for (lock <- locks)
  {
    if(!lockmap.contains(lock.key))
    {
      lockmap += (lock.key -> new Lock_status()) // initialize to a free lock
      var rollback:Any = store.get(lock.key)
      assert(!rollback_value(Tid).contains(lock.key))
      rollback_value(Tid) += (lock.key->rollback)
    }
  }

  locklist_map += (Tid -> locks)

  var flag: Boolean = true

  for (lock <- locks)
  {
    if (lock.rw == 'r')
    {
      if (lockmap(lock.key).writer_list)
      {
        flag = false
      }
    }
    else if (lock.rw == 'w')
    {
      if (lockmap(lock.key).writer_list || lockmap(lock.key).reader_list.length > 0)
      {
        flag = false
      }
    }
  }

  if (flag == false)
  {
    if(disconnected)
      sender ! FAILED
    else
      sender ! 0
  }
  else
  { 
    var lockmap_backup = lockmap

    for (lock <- locks)
    {
      if(lock.rw == 'r')
      {
        lockmap(lock.key).reader_list += Tid
      }
      else
      {
        lockmap(lock.key).writer_list = true
      }
    }
    
    for (i <- 0 until 5)
    {
      if(ID % 5 != i)
      {
        var id = ID/5*5+i
        val future = replicas.get(id) ? Update_lock_status(lockmap)
        var result1 = Await.result(future,timeout.duration).asInstanceOf[Any]
        var future1 = replicas.get(id) ? Update_lock_list(locklist_map)
        var result2 = Await.result(future1,timeout.duration).asInstanceOf[Any]
        var future2 = replicas.get(id) ? Update_Rollback(rollback_value)
        var result3 = Await.result(future2,timeout.duration).asInstanceOf[Any]
        var result = result1.asInstanceOf[Int]
        if (result != 1)
          flag = false
      }
    }

    if (!flag)
    {
      lockmap = lockmap_backup
      if(disconnected)
        sender ! FAILED
      else
        sender ! 0   
    }
    else 
    {
      if(disconnected)
        sender ! FAILED
      else
        sender ! 1
    }
  }
}
def handle_grap_locks(locks:ListBuffer[Lock_Obj],Tid:Int) =
{

  rollback_value += (Tid->new HashMap[Int,Any])
  for (lock <- locks)
  {
    if(!lockmap.contains(lock.key))
    {
      lockmap += (lock.key -> new Lock_status()) // initialize to a free lock
      var rollback:Any = store.get(lock.key)
      assert(!rollback_value(Tid).contains(lock.key))
      rollback_value(Tid) += (lock.key->rollback)
    }
  }

  locklist_map += (Tid -> locks)

  var flag: Boolean = true

  for (lock <- locks)
  {
    if (lock.rw == 'r')
    {
      if (lockmap(lock.key).writer_list)
      {
        flag = false
      }
    }
    else if (lock.rw == 'w')
    {
      if (lockmap(lock.key).writer_list || lockmap(lock.key).reader_list.length > 0)
      {
        flag = false
      }
    }
  }

  if (flag == false)
  {
    if(disconnected)
      sender ! FAILED
    else
      sender ! 0
  }
  else
  {
    for (lock <- locks)
    {
      if(lock.rw == 'r')
      {
        lockmap(lock.key).reader_list += Tid
      }
      else
      {
        lockmap(lock.key).writer_list = true
      }
    }
    if(disconnected)
      sender ! FAILED
    else
      sender ! 1
  }
  

}

def handle_abort(Tid:Int) = 
{
  if(rollback_value.contains(Tid))
  {
    var m1 = rollback_value(Tid)

    for ((key,value) <- m1)
    {
      store.put(key,value)
    }
    rollback_value -= Tid
  }
  //release all the locks

  if(locklist_map.contains(Tid))
  {
    var locklist = locklist_map(Tid)

    for (lock <- locklist)
    {
      if (lock.rw =='r')
      {
        lockmap(lock.key).reader_list -= Tid
      }
      else
      {
        lockmap(lock.key).writer_list = false
      }
    }

    locklist_map -= Tid
  }

}



def handle_commit(Tid:Int) = 
{
  //just release locks
  var locklist = locklist_map(Tid)
  for (lock <- locklist)
  {
    if (lock.rw == 'r')
    {
      lockmap(lock.key).reader_list -= Tid
    }
    else
    {
      lockmap(lock.key).writer_list = false
    }
  }

  locklist_map -= Tid

  }

}

object KVStore {
  def props(): Props = {
     Props(classOf[KVStore])
  }
}

