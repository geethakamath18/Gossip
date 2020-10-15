// #time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic
open Akka.FSharp
open System.Diagnostics

let system = System.create "system" <| Configuration.defaultConfig()
let mutable flag = false
let mutable n=0 //Number of nodes
let mutable total = 0
let mutable time =  System.Diagnostics.Stopwatch() 
let mutable algorithm = ""
let mutable waittime = 0

let mutable echoActorsPS=new List<IActorRef>()

//Creating Master Actors
let mutable masterActor=new List<IActorRef>()
let mutable masterActorPS=new List<IActorRef>()

let mutable m = Map.empty<int, List<int>>

let mutable sumMap = Map.empty<int, float>
let mutable weightMap = Map.empty<int, float>


let mutable t = ""
let mutable finish = new List<int>()

let random = Random()
let mutable rnd=0


let wait() = 
    for i = 1 to 100000 do 
        waittime <- waittime+1 
let doublewait() = 
    for i = 1 to 1300 do 
        for j = 1 to 100 do 
            waittime <- waittime+1


type ActorMessageType = 
    | Neigbhour of int * int 
    | Self of int * int 
    | Topo of string * int
    | Finished of int
    | Topology of string
    | Receive of float * float * int
    | Selfsender of float * float * int

let mutable echoActors=new List<IActorRef>()

let mutable heard = new List<int>()


let gossipAlgo (mailbox:Actor<_>) =
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
                    mailbox.Self <! Self(num, 0)
                        
                    if t = "line" then 
                        doublewait()
                    else 
                        wait()
                    if not (finish.Contains(num)) then    
                        echoActors.[kee] <! Neigbhour(kee, 1)
                    else 
                        let mutable foundNeibhour = false
                        for i = 0 to heard.Count - 1 do 
                            if not (finish.Contains(heard.[i])) then
                                foundNeibhour <- true
                                echoActors.[heard.[i]] <! Neigbhour(heard.[i], 1)
                        while finish.Contains(rand) && not foundNeibhour do
                            rand <- random.Next(0,n)
                        if not foundNeibhour then
                            echoActors.[rand] <! Neigbhour(rand, 1)
     
            |Neigbhour (num, inc) ->
                count <- count+inc
                if count = 5 then
                    masterActor.[0] <! Finished(num)
                if count = 1 then 
                    total <- total+1 
                    heard.Add(num)

                if t = "line" then 
                    doublewait()
                else 
                    wait()
                mailbox.Self <! Self(num, 0)
            |_->
                printfn "not a valid message type"
            return! loop ()
            
        }
        loop ()

let pushSumAlgo (mailbox:Actor<_>) = 
    // printfn "Entered actor with random : %d" rnd
    let mutable count=0
    let mutable nodeCount = 0 
    let mutable ratioList=new List<float>()
    let mutable weight=1
    
    let rec loop () = actor {
        let! msg = mailbox.Receive () //Recieving messages from the mailbox
        
        match msg with
        | Receive(s_rec, w_rec, nodeNumber) ->
            let mutable totalSum = 0 |> float
            let mutable totalWeight = 0|>float
            totalSum <- sumMap.[nodeNumber]  + s_rec
            totalWeight <- weightMap.[nodeNumber] + w_rec 

            totalSum <- totalSum / 2.0
            totalWeight <- totalWeight / 2.0 

            ratioList.Add(totalSum/totalWeight)

            sumMap <- sumMap.Add(nodeNumber, totalSum)
            weightMap <- weightMap.Add(nodeNumber, totalWeight)

            let mutable a = m.[nodeNumber].Count-1
            let mutable rand = random.Next(0,a)

            if finish.Count >= n-2 then
                flag <- true

            let mutable kee = m.[nodeNumber].[rand]
 
            if not (finish.Contains(nodeNumber)) || finish.Count >= n-2 then
                mailbox.Self <! Selfsender(s_rec, w_rec, nodeNumber)

            wait()
            if not (finish.Contains(kee)) then
                echoActorsPS.[kee] <! Receive(totalSum, totalWeight, kee)
            else 
                let mutable geetha = 0 
                while finish.Contains(kee) do
                    kee <- random.Next(0,n-1)
 
                echoActorsPS.[kee] <! Selfsender(s_rec, w_rec, kee)
        | Selfsender(s_rec, w_rec, nodeNumber) ->

            if ratioList.Count = 1 then 
                total <- total + 1


            if(ratioList.Count>=3 && ((s_rec/w_rec)-ratioList.[ratioList.Count-1])<(10.0**(-10.0)) && ((s_rec/w_rec)-ratioList.[ratioList.Count-2])<(10.0**(-10.0)) && ((s_rec/w_rec)-ratioList.[ratioList.Count-3])<(10.0**(-10.0)) && not (finish.Contains(nodeNumber))) then //Stopping Condition
                let mutable t = s_rec/ w_rec

                masterActor.[0]<! Finished(nodeNumber)

            wait()
            if not (finish.Contains(nodeNumber)) then
                mailbox.Self <! Receive(s_rec, w_rec, nodeNumber)
        | _->
            printfn "not a valid message"

        return! loop ()
        
    }
    loop ()   

let spawnActorsForGossip() = 
    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties gossipAlgo
        echoActors.Add(actor) 

let spawnActorsForPushSum() = 
        for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties pushSumAlgo
        echoActorsPS.Add(actor) 

let assignSumWeight(n) = 
    for i = 0 to n-1 do 
        sumMap <- sumMap.Add(i,float(i))
        weightMap <- weightMap.Add(i, 1.0)

let lineTopology(n:int) = 

    match algorithm with 
    | "gossip" ->
        spawnActorsForGossip()
    | "push-sum" -> 
        spawnActorsForPushSum()
        assignSumWeight(n)
    | _ -> 
        printfn "Enter Proper topology Gossip or Push-Sum"

    for i = 0 to n-1 do 
        let list = new List<int>()
        if i = 0 then 
            list.Add(i+1)
        else if i = n-1 then 
            list.Add(i-1)

        else 
            list.Add(i+1)
            list.Add(i-1)

        m <- m.Add(i, list)

    let rnd = random.Next(0,n-1)

    time <- Stopwatch.StartNew()
    match algorithm with
    |"gossip" -> 
        echoActors.[rnd] <! Neigbhour(10, 1)
    |"push-sum" ->
        echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)
    |_ ->
        printfn "not a proper algorithm"


let fullTopology(n: int)=

    match algorithm with 
    |"gossip" ->
        spawnActorsForGossip()
    |"push-sum"-> 
        spawnActorsForPushSum()
    |_ -> 
        printfn "Enter Proper topology Gossip or Push-Sum"

    for i in [0 .. n] do
        let l = new List<int>()
        for j in [0 .. n] do
            if j <> i then 
                l.Add(j)
        m<-m.Add(i,l)
        // printfn "%A" m
    let rnd = random.Next(0,n-1)
    assignSumWeight(n)

    time <- Stopwatch.StartNew()

    match algorithm with
    |"gossip" -> 
        echoActors.[rnd] <! Neigbhour(10, 1)
    |"push-sum" ->
        echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)
    |_ ->
        printfn "not a proper algorithm"


let twoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int
    n <- sq * sq
    match algorithm with 
    | "gossip" ->
        spawnActorsForGossip()
    | "push-sum" -> 
        spawnActorsForPushSum()
        assignSumWeight(n)
    | _ -> 
        printfn "Enter Proper topology Gossip or Push-Sum"

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
    
    let rnd = random.Next(0,n-1)

    time <- Stopwatch.StartNew()
    match algorithm with
    |"gossip" -> 
        echoActors.[rnd] <! Neigbhour(10, 1)
    |"push-sum" ->
        echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)
    |_ ->
        printfn "not a proper algorithm"


let imptwoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int
    n <- sq * sq
    let randomlist = new List<int>()
    match algorithm with 
    |"gossip" ->
        spawnActorsForGossip()
    |"push-sum" -> 
        spawnActorsForPushSum()
        assignSumWeight(n)
    |_ -> 
        printfn "Enter Proper Algorithm gossip or push-sum"

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

        list.Add(number)
        m <- m.Add(i, list)
    
    let rnd = random.Next(0,n-1)

    time <- Stopwatch.StartNew()
    match algorithm with
    |"gossip" -> 
        echoActors.[rnd] <! Neigbhour(10, 1)
    |"push-sum" ->
        echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)
    |_ ->
        printfn "not a proper algorithm"


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
            |_ -> printfn "improper topology select one of these full, 2D, line, imp2D"
        |Finished (node) ->
            if not (finish.Contains(node)) then
                finish.Add(node)
            if finish.Count >= n-2 then
                flag <- true
        |_ -> printfn "unkown messageType recived"
        return! masterloop ()
    }      
    masterloop()

let main (args:string []) =
    n<-(int) args.[1] //Setting the value of number of nodes
    algorithm <- args.[3]
    t<-args.[2]
    let mast = spawn system "master" master    
    masterActor.Add(mast)
    masterActor.[0] <! Topo (t, n)

    while not flag do 
        wait()
    let mutable delay = 0

    doublewait()

    time.Stop()
    printfn "Time taken for %A topology to converge for %A is %f" t algorithm time.Elapsed.TotalMilliseconds
    0
let args = fsi.CommandLineArgs 
match args.Length with //Checking number of parameters
    | 4 -> main args    
    | _ ->  failwith "You need to pass two parameters!"
