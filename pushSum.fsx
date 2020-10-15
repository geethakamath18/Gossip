// dotnet fsi --langversion:preview push.fsx 100 full gossip

#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic
open Akka.FSharp

//let z=10.0**(-10.0)
let system = System.create "system" <| Configuration.defaultConfig()
let mutable flag = false
let mutable n=0
type ProcessorMessage = ProcessJob of int * string * int    
type MasterMessage = MasterJob of int * int * int * int

type ActorMessageTypePS = 
    |   Neigbhour of int * int * int
    |   Self of int * int * int
    |   Topo of string * int
    |   Finished of int
    |   Topology of string

//Creating Actor System
//let mutable echoActors=new List<IActorRef>()
let mutable echoActorsPS=new List<IActorRef>()

//Creating Master Actors
//let mutable masterActor=new List<IActorRef>()
let mutable masterActorPS=new List<IActorRef>()

let mutable m = Map.empty<int, List<int>>

let mutable ak=0

let mutable finish = new List<int>()

let random = Random()
let mutable rnd=0
// Actor model for Push Sum Algorithm
let echoPS (mailbox:Actor<_>) = 
    printfn "Entered actor with random : %d" rnd
    let mutable count=0
    let mutable s=0//Get actor number
    
    let mutable a=0
    let mutable gee=0
    let mutable w=1
    let mutable ratioList=new List<float>()
    let mutable ratio=float(s)
    
    let rec loop () = actor {
        let! msg = mailbox.Receive () //Recieving messages from the mailbox
        //let sender = mailbox.Sender() 
        let mutable delay = 0   
        // let mutable actornumber = 0
        //printfn "%d" rnd
        match msg with
        |Self (w_received, s_received, num) ->
            //count <- count + inc
            s<-s+s_received
            w<-w+w_received
            ratioList.Add(float(s/w))
            a<-m.[num].Count-1
            rnd<-random.Next(0,a)
            
            gee<-m.[num].[rnd]
            printfn "Raodm chosen inside actor: %d" gee
            
            for k = 1 to 1000000 do  //Delay
                delay <- k 
            if not (finish.Contains(num)) then    //transmitting to rabdom neighbour
                echoActorsPS.[gee] <! Neigbhour(s/2, w/2, gee)


            else 
                let mutable dummyCount = 0
                for i = 0 to m.[num].Count - 1 do 
                    if finish.Contains(m.[num].[i]) then
                        dummyCount <- dummyCount + 1 
                if dummyCount = m.[num].Count then 
                    flag <- true 
            
        
        |Neigbhour (s_send, w_send, num) ->
            //count <- count+inc
            s<-(s+s_send)
            w<-(w+w_send)
            // ratioPrev<-float(ratioCurrent)
            //ratioCurrent<-float(s/w)
            ratioList.Add(float(s/w))
            a<-m.[num].Count-1

            rnd<-random.Next(0,a)
            printfn "Raodm chosen inside actor 2: %d" rnd
            gee<-m.[num].[rnd]            
            if(ratioList.Count>=3 && (ratioList.[ratioList.Count-1]-float(s/w))>(10.0**(-10.0)) && (ratioList.[ratioList.Count-2]-float(s/w))>(10.0**(-10.0)) && (ratioList.[ratioList.Count-3]-float(s/w))>(10.0**(-10.0))) then //Stopping Condition
                masterActorPS.[0]<!Finished (num)
            //if(ratioList.Count>2) then    //count<-count+1
                //if(count<>3) then
                
            // if cou then
            //     masterActorPS.[0] <! Finished(num)
            // if count = 1 then    
            //     total <- total+1 
            //     list.Add(num)   
            // printfn "node %d recived first gossip and total is %d" num total

            for k = 1 to 10000 do 
                delay <- k 
            mailbox.Self <! Self(0, 0, num)
            // mailbox.Self <! Self(num, 0)

        return! loop ()
        
    }
    loop ()



let fullTopology(n: int)=
    for i in [0 .. n] do
        let properties = string(i)
        let actor = spawn system properties echoPS
        echoActorsPS.Add(actor) 
    for i in [0 .. n] do
        let l = new List<int>()
        for j in [0 .. n] do
            if j <> i then 
                l.Add(j)
        m<-m.Add(i,l)
        // printfn "%A" m
    rnd <- random.Next(0,n-1)
    printfn "Random chosen: %d" rnd
    //echoActors.[rnd] <!rnd, 0)
    echoActorsPS.[rnd] <! Self(1,rnd,rnd) //First call


let masterPS(mailbox: Actor<_>)= 
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        // printfn "%A" n
        let mutable dummy = 0
        match msg with
        |Topo (topo, n) ->
                fullTopology(n)
                // echoActorsPS.[0] <! Self (0, 1) 
        |Finished (node) ->
                if not (finish.Contains(node)) then
                    finish.Add(node)
                    printfn "node %d finished" node
                if finish.Count >= n then
                    flag <- true
        
        return! masterloop ()
    }      
    masterloop()

let main (args:string []) =
    n<-(int) args.[1] //Setting the value of number of nodes
    let t=args.[2] //Topology
    let a=args.[3] //algorithm
    // let mast = spawn system "master" master 
    let mast = spawn system "master" masterPS    
    masterActorPS.Add(mast)
    masterActorPS.[0] <! Topo (t, n)
    let mutable k = 0
    while not flag do 
        k <- k+1
    0

let args = fsi.CommandLineArgs 
match args.Length with //Checking number of parameters
    | 4 -> main args    
    | _ ->  failwith "You need to pass 3 parameters!"
