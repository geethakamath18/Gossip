//dotnet fsi --langversion:preview project.fsx 10 full gossip


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
let mutable n=0
type ProcessorMessage = ProcessJob of int * string
type MasterMessage = MasterJob of int * int * int * int

type ActorMessageType = 
    | Message of string
let mutable flag=0
let mutable echoActors=new List<IActorRef>()
let mutable m = Map.empty<int, List<int>>

let mutable finish = new List<int>()
// let getrandomitem (a: int List) =  
//     let rnd = System.Random()  
//     fun (combos : int list) -> List.nth a (rnd.Next(a.Length))

// let getrandomitem () =  
//   let rnd = System.Random()  
//   fun (combos : string list) -> List.nth combos (rnd.Next(combos.Length))
//Actor system for gossip
let echo (mailbox:Actor<_>) =
        let mutable count=0
        let rec loop () = actor {
            let! msg = mailbox.Receive () //Recieving messages from the mailbox
            let i=int(msg)
            // printfn "%d" i
            // printfn "%A is the message" msg
            count<-count+1
            printfn "%d :%d" i count
            if count=10 then
                printfn "REACHED LIMIT" 
                finish.Add(i)
                system.Stop(echoActors.[i])
                let a=m.[i]
                let objrandom =Random()

                let mutable ran = objrandom.Next(0,a.Count-1)
                while finish.Contains(ran) && finish.Count <> n - 2 do
                    printfn "inside if"
                    ran <- objrandom.Next(0,a.Count-1)
                if finish.Count <> n - 2 then 
                    echoActors.[ran] <! ran
                
                // system.Stop(echoActors.[i])
            else
                let a=m.[i]
                // let b=a.Count
                let objrandom =Random()
                let mutable ran = objrandom.Next(0,a.Count-1)
                // echoActors.[ran] <! ran
                // printfn " B ISSSSSSSSSSSSSSSSSS  %A" a
                //let b= a.Length      
                //let objrandom =Random()
                //let mutable ran = objrandom.Next(a.Length)
                // let mutable z=shuffleR(a)
                // let mutable it=a|> shuffleR (Random ()) |> Seq.head
                // printfn "TRANSMITTING MESSAGE"
                let mutable ran = objrandom.Next(0,a.Count-1)
                while finish.Contains(ran) && finish.Count <> n- 2 do
                    printfn "inside else"
                    ran <- objrandom.Next(0,a.Count-1)
                
                if finish.Count <> n - 2 then
                    echoActors.[ran] <! ran
            return! loop ()
            
        }
        loop ()



module Topology=
    let genLineTopology (n: int)=
        printfn "GEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"
        let echoActors =        //Spawning Actors
                [1 .. n]    
                |> List.map(fun id ->   let properties = string(id) 
                                        spawn system properties echo)                               
        let mutable line = Map.empty
        for i in [0 .. n-1] do 
            let list = new List<int>()
            if i = 0 then 
                list.Add(i+1)
                line <- line.Add(i, list)
            else if i = n-1 then 
                list.Add(i-1)
                line <- line.Add(i, list)
            else 
                list.Add(i+1)
                list.Add(i-1)
                line <- line.Add(i, list)
        let objrandom =Random()
        let rnd = objrandom.Next(1,n)
        echoActors.[rnd] <! rnd
    // let gen2DGridTopology=
    
    let fullTopology(n: int)=
        for i in [0 .. n-1] do
            let properties = string(i)
            let actor = spawn system properties echo
            echoActors.Add(actor) 
        // let mutable m = Map.empty
        // let x = ceil(sqrt(float(n))) |> int
        // let sk = x * x
        for i in [0 .. n-1] do
            let l = new List<int>()
            for j in [0 .. n-1] do
                if j <> i then 
                    l.Add(j)
            m<-m.Add(i,l)
        let objrandom =Random()
        let rnd = objrandom.Next(1,n)
        echoActors.[rnd] <! rnd
    // let imperfect2DGridTopology=

module Gossip=
    
    
    let master (mailbox: Actor<_>) = 
        let rec masterloop() = actor{
            let! ProcessJob(n,t) =  mailbox.Receive ()
            // printfn "%A" n
            printfn "DEBUG"
            match t with
            | "line" -> Topology.genLineTopology(n)
            | "2Dgrid" -> printfn "2D Grid"
            | "full" -> Topology.fullTopology(n)
            | "imperfect2D" -> printfn "Imperfect"
            | _ -> failwith "Check topology"                            
            return! masterloop ()
        }      
        masterloop()
    
    let init(n:int, t: string)=
        printfn "DEBUG GOSSIP"
        let masterActor = spawn system "master" master //Spawning the master/Supervisor actor
        masterActor <! ProcessJob(n,t)
        
        

module pushSum=
    let init(n:int, t: string)=
        printfn "CALLED PUSH SUM"




//main function
let main (args:string []) =
    n<-(int) args.[1] //Setting the value of number of nodes
    let t=args.[2]
    let a=args.[3]

    match t with
    | "line" -> printfn "Line"
    | "2Dgrid" -> printfn "2D Grid"
    | "full" -> printfn "Full network"
    | "imperfect2D" -> printfn "Imperfect"
    | _ -> failwith "Check topology"

    match a with
    | "pushSum" -> pushSum.init(n,t)
    | "gossip" -> Gossip.init(n,t)
    | _ -> failwith "Check algorithm"
    let mutable i = 0
    while flag=0 do
        i <- i+1
    0

let args = fsi.CommandLineArgs 
match args.Length with //Checking number of parameters
    | 4 -> main args    
    | _ ->  failwith "You need to pass two parameters!"
