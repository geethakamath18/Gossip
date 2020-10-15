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

let system = System.create "system" <| Configuration.defaultConfig()
let mutable flag = false
let mutable n=0

type ProcessorMessage = ProcessJob of int * string * int    
type MasterMessage = MasterJob of int * int * int * int

type ActorMessageType = 
    | Neigbhour of int * int 
    | Self of int * int 
    | Topo of string * int
    | Finished of int
    | Topology of string

let mutable echoActors=new List<IActorRef>() //Creating an array of actors
let mutable m = Map.empty<int, List<int>>
let mutable masterActor=new List<IActorRef>() //Creating array of master actors
let mutable finish = new List<int>() // Array to maintain info about comverged nodes

let random = Random()
let echo (mailbox:Actor<_>) =
        let mutable count=0
        let rec loop () = actor {
            let! msg = mailbox.Receive () //Recieving messages from the mailbox
            let sender = mailbox.Sender() 
            let mutable delay = 0   
            // let mutable actornumber = 0
            match msg with
            |Self (num, inc) ->
                count <- count + inc
                //printfn "count:%d node:%d" count num
                if count >= 0 && count <= 10 then 
                    let mutable rand = random.Next(0, n-1)
                    while finish.Contains(m.[num].[rand]) do 
                        rand <- random.Next(0, n)
                    echoActors.[m.[num].[rand]] <! Neigbhour(rand, 1)
                    for k = 1 to 10000 do 
                        delay <- k + 1
                    echoActors.[num] <! Self(num, 0)

            |Neigbhour (num, inc) ->
                count <- count+inc
                if count >= 10 then
                    masterActor.[0] <! Finished(num)
                if count = 1 then 
                    printfn "node %d recived first gossip" num
                    mailbox.Self <! Self(num, 0) // 0?????

            return! loop ()
            
        }
        loop ()
let fullTopology(n: int)=
    for i in [0 .. n] do
        let properties = string(i) //For every actor, passing actor ID
        let actor = spawn system properties echo
        echoActors.Add(actor) 
    for i in [0 .. n] do
        let l = new List<int>()
        for j in [0 .. n] do
            if j <> i then 
                l.Add(j)
        m<-m.Add(i,l)
        printfn "%A" m
    let rnd = random.Next(0,n-1)
    echoActors.[rnd] <! Self(rnd, 0)

let master (mailbox: Actor<_>) = 
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        // printfn "%A" n
        let mutable dummy = 0
        match msg with
        |Topo (topo, n) -> //Match passed topoligy against pattern matching
                fullTopology(n)
                // echoActors.[0] <! Self (0, 1) 
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
    let t=args.[2]
    let a=args.[3]
    let mast = spawn system "master" master    
    masterActor.Add(mast) // Adding master reference to lsit of master actors
    masterActor.[0] <! Topo (t, n) //Only master Actor[0] is active
    let mutable k = 0
    while not flag do 
        k <- k+1
    0
let args = fsi.CommandLineArgs 
match args.Length with //Checking number of parameters
    | 4 -> main args    
    | _ ->  failwith "You need to pass two parameters!"