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
let mutable total = 0
let mutable limit = true
type ProcessorMessage = ProcessJob of int * string * int    
type MasterMessage = MasterJob of int * int * int * int

type ActorMessageType = 
    | Neigbhour of int * int 
    | Self of int * int 
    | Topo of string * int
    | Finished of int
    | Topology of string
let mutable echoActors=new List<IActorRef>()
let mutable m = Map.empty<int, List<int>>
let mutable masterActor=new List<IActorRef>()
let mutable finish = new List<int>()

let random = Random()
let echo (mailbox:Actor<_>) =
        let mutable count=0
        let rec loop () = actor {
            let! msg = mailbox.Receive () //Recieving messages from the mailbox
            let sender = mailbox.Sender() 
            let mutable list = new List<int>()
            let mutable delay = 0   
            let mutable actornumber = 0
            match msg with
            |Self (num, inc) ->
                count <- count + inc
                // printfn "count:%d node:%d" count num
                if count >= 0 && count <= 10 then 
                    let mutable a = m.[num].Count-1
                    let mutable rand = random.Next(0,a)
                    // printfn "length of sequenc %d" m.[num].Count
                    let mutable kee = m.[num].[rand]
                    
                    // while finish.Contains(m.[num].[rand])do  
                    //     a <- m.[num].Count-1
                    //     rand <- random.Next(0,a)
                    //     kee <- m.[num].[rand]

                    if not (finish.Contains(num)) then    
                        echoActors.[kee] <! Neigbhour(kee, 1)
                    else 
                        while finish.Contains(rand) || finish.Count >= n-1 do
                            rand <- random.Next(0,n-1)
                        echoActors.[rand] <! Neigbhour(rand, 1)    
                    //     echoActors.[kee] <! Neigbhour(kee, 0)
                    for k = 1 to 10000 do 
                        delay <- k 
                    mailbox.Self <! Self(num, 0)
            |Neigbhour (num, inc) ->
                count <- count+inc
                if count = 10 then
                    masterActor.[0] <! Finished(num)
                if count = 1 then 
                    total <- total+1 
                    list.Add(num)
                    printfn "node %d recived first gossip and total is %d" num total
                for k = 1 to 10000 do 
                    delay <- k 
                mailbox.Self <! Self(num, 0)

            return! loop ()
            
        }
        loop ()

let imptwoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int
    n <- sq * sq
    let randomlist = new List<int>()
    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties echo
        echoActors.Add(actor) 
    printfn "twoDgrid %d " echoActors.Count
    for i in [0 .. n-1] do 
        let list = new List<int>()
        if i-1 >= 0 && i%sq <> 0 then 
            list.Add(i-1)
        if (i+1) % sq <> 0 then 
            list.Add(i+1)
        if i+4 < n then 
            list.Add(i+4)
        if i-4 >= 0 then 
            list.Add(i-4)
        let random = System.Random()
        let mutable number = random.Next(n-1)
        while list.Contains(number) || number = i do
            number <- random.Next(0, n-1)
        printfn "random :%d" number
        // randomlist.Add(number)
        list.Add(number)
        m <- m.Add(i, list)
    
    for i in [0 .. m.Count-1] do
        printfn "%d:%A" i m.[i]
    let rnd = random.Next(0,n-1)
    printfn "random %d " rnd
    let mutable start = n / 2
    printfn "sumne %d" start
    echoActors.[start] <! Self(start, 0)


let master (mailbox: Actor<_>) = 
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        // printfn "%A" n
        let mutable dummy = 0
        match msg with
        |Topo (topo, n) ->
                // fullTopology(n)
                imptwoDgrid(n)
                //imperfectTwoDgrid(n)
                //echoActors.[0] <! Self (0, 1) 
        |Finished (node) ->
                if not (finish.Contains(node)) then
                    finish.Add(node)
                    printfn "node %d finished and total nodes is %d" node finish.Count
                if finish.Count >= n-1 then
                    flag <- true
        
        return! masterloop ()
    }      
    masterloop()

let main (args:string []) =
    n<-(int) args.[1] //Setting the value of number of nodes
    let t=args.[2]
    let a=args.[3]
    let mast = spawn system "master" master    
    masterActor.Add(mast)
    masterActor.[0] <! Topo (t, n)
    let mutable k = 0
    while not flag do 
        k <- k+1
    0
let args = fsi.CommandLineArgs 
match args.Length with //Checking number of parameters
    | 4 -> main args    
    | _ ->  failwith "You need to pass two parameters!"
