package transaction

import akka.actor.{Actor, ActorSystem, ActorRef, Props}
import collection.mutable.HashMap 
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext
import akka.event.Logging

sealed trait LoadMasterAPI
case class App_check() extends LoadMasterAPI
case class App_deposit(value:Int) extends LoadMasterAPI
case class App_withdraw(value:Int) extends LoadMasterAPI
case class App_transfer(target:Int,value:Int) extends LoadMasterAPI
case class View(endpoints:Seq[ActorRef]) extends LoadMasterAPI
case class Viewclients(endpoints:Seq[ActorRef]) extends LoadMasterAPI
case class App_Begin_transaction() extends LoadMasterAPI
case class App_End_transaction() extends LoadMasterAPI
case class Viewreplicas(replicas:Seq[ActorRef]) extends LoadMasterAPI
case class ViewID(id:Int) extends LoadMasterAPI

object LoadMaster {

	val system = ActorSystem("transaction")
	val server = for (i <- 0 until 50)
        yield system.actorOf(KVStore.props(),"Server" + i)
	val clients = for (i <- 0 until 10)
        yield system.actorOf(TransactionClient.props(system,i,server), "LockClient" + i)
    val applications = for (i <- 0 until 10)
        yield system.actorOf(TransactionApp.props(i,clients(i)), "Application" + i)
    for (client <- clients)
    	client ! View(applications)
    for (client <- clients)
        client ! Viewclients(clients)
    for(i <- 0 until 50)
        server(i) ! ViewID(i)
    for(s <- server)
        s ! Viewreplicas(server)
/*
    for(i <- 0 until 10)
    {
        for(j <- 0 until 5)
        {
            var m = 0
            for(k <- 0 until 5)
            {
                if(j != k)
                {
                    replica(m) = server(5*i+k)
                    println(m+" "+(5*i+k))
                    m = m+1

                }
            }
            server(5*i+j) ! Viewreplicas(replica)
        }
    }
*/


    //lockserver! View(clients)

    def main(args: Array[String]): Unit = run()

    def run(): Unit = {

/*
        applications(0) ! App_Begin_transaction()
        applications(0) ! App_deposit(5)
        applications(0) ! App_End_transaction()

        Thread.sleep(2000)

        server(0) ! Disconnect_replica()
        server(1) ! Disconnect_replica()

        applications(0) ! App_Begin_transaction()
        applications(0) ! App_deposit(5)
        applications(0) ! App_End_transaction()

        Thread.sleep(2000)

        server(0) ! Connect_replica()
        server(1) ! Connect_replica()
        server(3) ! Disconnect_replica()
        server(4) ! Disconnect_replica()

        applications(0) ! App_Begin_transaction()
        applications(0) ! App_deposit(5)
        applications(0) ! App_End_transaction()

        Thread.sleep(2000)

        server(3) ! Connect_replica()
        server(4) ! Connect_replica()
        server(2) ! Disconnect_replica()

        applications(0) ! App_Begin_transaction()
        applications(0) ! App_deposit(5)
        applications(0) ! App_End_transaction()

        Thread.sleep(2000)
*/  // test case 2
        


        for(i <- 0 until 10)
        {
            applications(i) ! App_Begin_transaction()
            applications(i) ! App_check()
            applications(i) ! App_deposit(100)
            applications(i) ! App_withdraw(60)
            applications(i) ! App_End_transaction()
        }
        Thread.sleep(2000)

        for(i <- 0 until 5)
        {
            applications(2*i) ! App_Begin_transaction()
            applications(2*i) ! App_transfer(2*i+1,20)
            applications(2*i) ! App_End_transaction()
        }

        Thread.sleep(2000)


        for(i <- 0 until 5)
        {
            applications(2*i+1) ! App_Begin_transaction()
            applications(2*i+1) ! App_check()
            applications(2*i+1) ! App_End_transaction()
        }
        
        Thread.sleep(2000)

        for(i <- 0 until 5)
        {
            applications(2*i) ! App_Begin_transaction()
            applications(2*i) ! App_check()
            applications(2*i+1) ! App_Begin_transaction()
            applications(2*i+1) ! App_transfer(2*i,10)
            applications(2*i+1) ! App_End_transaction()
            applications(2*i) ! App_End_transaction()
        }

        Thread.sleep(2000)

        for(i <- 0 until 10)
        {
            applications(i) ! App_Begin_transaction()
            applications(i) ! App_check()
            applications(i) ! App_End_transaction()
        }
        Thread.sleep(2000)
  //test case 1
    	system.shutdown()
    }
}