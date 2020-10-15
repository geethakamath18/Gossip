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
let mutable awake = 0
let mutable time = 0
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
            let mutable list = new List<int>() //
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
                    mailbox.Self <! Self(num, 0)
                        
                    for k = 1 to 1000000 do //delay
                        delay <- k 
                    if not (finish.Contains(num)) then    //transmitting to rabdom neighbour
                        echoActors.[kee] <! Neigbhour(kee, 1)
                    else 
                        let mutable dummyCount = 0
                        for i = 0 to m.[num].Count - 1 do 
                            if finish.Contains(m.[num].[i]) then
                                dummyCount <- dummyCount + 1 
                        if dummyCount = m.[num].Count then 
                            flag <- true 
                        // awake <- awake + 1
                        // printfn "awake %d" awake
                        // if awake >= n then 
                        //     flag <- true
            |Neigbhour (num, inc) ->
                count <- count+inc
                if count = 10 then //stopping condition
                    masterActor.[0] <! Finished(num)
                if count = 1 then //For printing
                    total <- total+1 
                    list.Add(num)
                    // printfn "node %d recived first gossip and total is %d" num total
                for k = 1 to 10000 do 
                    delay <- k 
                mailbox.Self <! Self(num, 0)

            return! loop ()
            
        }
        loop ()

let lineTopology(k:int) = 

    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties echo
        echoActors.Add(actor) 
    for i = 0 to k-1 do 
        let list = new List<int>()
        if i = 0 then 
            list.Add(i+1)
        else if i = n-1 then 
            list.Add(i-1)
            // m <- m.Add(i, list)
        else 
            list.Add(i+1)
            list.Add(i-1)
            // m <- m.Add(i, list)
        m <- m.Add(i, list)
    // for i in [0 .. m.Count-1] do
        // printfn "%d:%A" i m.[i]
    let rnd = random.Next(0,n-1)
    // printfn "random %d " rnd
    time <- System.DateTime.Now.Millisecond
    // printfn "time %u" time   
    echoActors.[rnd] <! Neigbhour(10, 1)

let fullTopology(n: int)=
    for i in [0 .. n] do
        let properties = string(i)
        let actor = spawn system properties echo
        echoActors.Add(actor) 
    for i in [0 .. n] do
        let l = new List<int>()
        for j in [0 .. n] do
            if j <> i then 
                l.Add(j)
        m<-m.Add(i,l)
        // printfn "%A" m
    let rnd = random.Next(0,n-1)
    time <- System.DateTime.Now.Millisecond
    // printfn "time %u" time
    echoActors.[rnd] <! Neigbhour(n/2, 1)


let twoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int
    n <- sq * sq
    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties echo
        echoActors.Add(actor) 
    // printfn "twoDgrid %d " echoActors.Count
    for i in [0 .. n-1] do 
        let list = new List<int>()
        if i-1 >= 0 && i%sq <> 0 then 
            list.Add(i-1)
        if (i+1) % sq <> 0 then 
            list.Add(i+1)
        if i+sq < n then 
            list.Add(i+sq)
        if i-sq >= 0 then 
            list.Add(i-sq)
        m <- m.Add(i, list)
    
    // for i in [0 .. m.Count-1] do
        // printfn "%d:%A" i m.[i]
    let rnd = random.Next(0,n-1)
    // printfn "random %d " rnd
    let mutable start = n / 2
    // printfn "sumne %d" start
    time <- System.DateTime.Now.Millisecond
    echoActors.[start] <! Neigbhour(start, 1)

let imptwoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int
    n <- sq * sq
    let randomlist = new List<int>()
    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties echo
        echoActors.Add(actor) 
    // printfn "twoDgrid %d " echoActors.Count
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
        // printfn "random :%d" number
        // randomlist.Add(number)
        list.Add(number)
        m <- m.Add(i, list)
    
    // for i in [0 .. m.Count-1] do
        // printfn "%d:%A" i m.[i]
    let rnd = random.Next(0,n-1)
    // printfn "random %d " rnd
    let mutable start = n / 2
    // printfn "sumne %d" start
    time <- System.DateTime.Now.Millisecond
    echoActors.[start] <! Neigbhour(start, 1)


let master (mailbox: Actor<_>) = 
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        // printfn "%A" n
        let mutable dummy = 0
        match msg with
        |Topo (topo, n) ->
            match topo with
            |"full" -> fullTopology(n)
            |"2D" -> twoDgrid(n)
            |"line" -> lineTopology(n)
            |"imp2D" -> imptwoDgrid(n)
        |Finished (node) ->
            if not (finish.Contains(node)) then
                finish.Add(node)
                //printfn "node %d finished and total nodes is %d" node finish.Count
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

    let a = System.DateTime.Now.Millisecond
    // printfn "a = %d" a 
    let b = System.DateTime.Now.Millisecond - time 
    // printfn "%d" time

    0
let args = fsi.CommandLineArgs 
match args.Length with //Checking number of parameters
    | 4 -> main args    
    | _ ->  failwith "You need to pass two parameters!"
