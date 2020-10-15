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
let mutable algorithm = "" //Input for algorithm
let mutable waittime = 0

let mutable echoActorsPS=new List<IActorRef>() // Creating a list for actors

//Creating Master Actors
let mutable masterActor=new List<IActorRef>() // Creating a master actor for Gossip
let mutable masterActorPS=new List<IActorRef>() // Creating a master actor for Push Sum

let mutable m = Map.empty<int, List<int>> // Creating a map for actors and the list of neighbours

let mutable sumMap = Map.empty<int, float> // Creating a map to store the s values for s for each of the actors
let mutable weightMap = Map.empty<int, float> // Creating a map to store the s values for w for each of the actors


let mutable t = "" // Variable to store the topology string
let mutable finish = new List<int>() // List to store nodes which have converged

let random = Random()
let mutable rnd=0

// Function to introduce delay
let wait() = 
    for i = 1 to 100000 do 
        waittime <- waittime+1 

// Function to introduce delay
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

let mutable echoActors=new List<IActorRef>() // Creating a list of actors

let mutable heard = new List<int>()

// Creating actor system for Gossip protocol
let gossipAlgo (mailbox:Actor<_>) =
        let mutable count=0
        let rec loop () = actor {
            let! msg = mailbox.Receive () //Recieving messages from the mailbox
            let sender = mailbox.Sender() 
            let mutable list = new List<int>()
            let mutable delay = 0   
            let mutable actornumber = 0
            match msg with
            |Self (num, inc) -> // Actor receiving the value of Actor ID and the value to increment
                count <- count + inc
                if count >= 0 && count <= 10 then // Checking count
                    let mutable a = m.[num].Count-1
                    let mutable rand = random.Next(0,a)
                    let mutable kee = m.[num].[rand] // Choosing random neighbour
                    mailbox.Self <! Self(num, 0) // Sending message to self
                        
                    if t = "line" then 
                        doublewait()
                    else 
                        wait()
                    if not (finish.Contains(num)) then  // Transmitting to a random neighbour  
                        echoActors.[kee] <! Neigbhour(kee, 1)
                    else 
                        let mutable foundNeibhour = false
                        for i = 0 to heard.Count - 1 do 
                            if not (finish.Contains(heard.[i])) then
                                foundNeibhour <- true
                                echoActors.[heard.[i]] <! Neigbhour(heard.[i], 1) // Checking which nodes are susceptible
                        while finish.Contains(rand) && not foundNeibhour do
                            rand <- random.Next(0,n)
                        if not foundNeibhour then
                            echoActors.[rand] <! Neigbhour(rand, 1)
     
            |Neigbhour (num, inc) ->
                count <- count+inc // Incrementing count
                if count = 5 then
                    masterActor.[0] <! Finished(num)
                if count = 1 then // Checking which nodes are susceptible
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


// Actor for Push Sum
let pushSumAlgo (mailbox:Actor<_>) = 
    let mutable count=0
    let mutable nodeCount = 0 
    let mutable ratioList=new List<float>()
    let mutable weight=1
    
    let rec loop () = actor {
        let! msg = mailbox.Receive () //Recieving messages from the mailbox
        
        match msg with
        | Receive(s_rec, w_rec, nodeNumber) -> // Getting the value of s and w
            let mutable totalSum = 0 |> float // Total sum
            let mutable totalWeight = 0|>float // Total weight
            totalSum <- sumMap.[nodeNumber]  + s_rec // Adding the value of received s to the current value of s
            totalWeight <- weightMap.[nodeNumber] + w_rec // Adding the value of received s to the current value of w

            totalSum <- totalSum / 2.0 // Halving the sum
            totalWeight <- totalWeight / 2.0 // Halving the weight

            ratioList.Add(totalSum/totalWeight) // Adding to a list

            sumMap <- sumMap.Add(nodeNumber, totalSum) // Adding sum to the map
            weightMap <- weightMap.Add(nodeNumber, totalWeight) // Adding weight to the map

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

            // checking conditions to see if the ratio has changed
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

// Spawning actors for Gossip protocol
let spawnActorsForGossip() = 
    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties gossipAlgo
        echoActors.Add(actor) 

// Spawning actors for Push Sum protocol
let spawnActorsForPushSum() = 
        for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties pushSumAlgo
        echoActorsPS.Add(actor) 

//Assign the value of s and w to each actor
let assignSumWeight(n) = 
    for i = 0 to n-1 do 
        sumMap <- sumMap.Add(i,float(i))
        weightMap <- weightMap.Add(i, 1.0)

// Creating the line network using maps
let lineTopology(n:int) = 

    match algorithm with // Spawning actors based on the algorithm
    | "gossip" ->
        spawnActorsForGossip()
    | "push-sum" -> 
        spawnActorsForPushSum()
        assignSumWeight(n)
    | _ -> 
        printfn "Enter Proper topology Gossip or Push-Sum"

    for i = 0 to n-1 do  // Creating the network
        let list = new List<int>()
        if i = 0 then 
            list.Add(i+1)
        else if i = n-1 then 
            list.Add(i-1)

        else 
            list.Add(i+1)
            list.Add(i-1)

        m <- m.Add(i, list) // Adding the actor number and list of neighbours

    let rnd = random.Next(0,n-1)

    time <- Stopwatch.StartNew()
    match algorithm with // Choosing algorithm and transmitting to a random neighbour
    |"gossip" -> 
        echoActors.[rnd] <! Neigbhour(10, 1)
    |"push-sum" ->
        echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)
    |_ ->
        printfn "not a proper algorithm"

// Creating the full network using maps
let fullTopology(n: int)=

    match algorithm with // Spawning actors based on the algorithm
    |"gossip" ->
        spawnActorsForGossip()
    |"push-sum"-> 
        spawnActorsForPushSum()
    |_ -> 
        printfn "Enter Proper topology Gossip or Push-Sum"

    for i in [0 .. n] do // Creating the network
        let l = new List<int>()
        for j in [0 .. n] do
            if j <> i then 
                l.Add(j)
        m<-m.Add(i,l) // Adding the actor number and list of neighbours
        // printfn "%A" m
    let rnd = random.Next(0,n-1)
    assignSumWeight(n) //Assigning sum weights to each actor

    time <- Stopwatch.StartNew()

    match algorithm with // Choosing algorithm and transmitting to a random neighbour
    |"gossip" -> 
        echoActors.[rnd] <! Neigbhour(10, 1)
    |"push-sum" ->
        echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)
    |_ ->
        printfn "not a proper algorithm"

// Creating the Imperfect 2D grid using maps
let twoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int
    n <- sq * sq
    match algorithm with // Spawning actors based on the algorithm
    | "gossip" ->
        spawnActorsForGossip()
    | "push-sum" -> 
        spawnActorsForPushSum()
        assignSumWeight(n)
    | _ -> 
        printfn "Enter Proper topology Gossip or Push-Sum"

    for i in [0 .. n-1] do  // Creating the network
        let list = new List<int>()
        if i-1 >= 0 && i%sq <> 0 then 
            list.Add(i-1)
        if (i+1) % sq <> 0 then 
            list.Add(i+1)
        if i+sq < n then 
            list.Add(i+sq)
        if i-sq >= 0 then 
            list.Add(i-sq)
        m <- m.Add(i, list) // Adding the actor number and list of neighbours
    
    let rnd = random.Next(0,n-1)

    time <- Stopwatch.StartNew()
    match algorithm with // Choosing algorithm and transmitting to a random neighbour
    |"gossip" -> 
        echoActors.[rnd] <! Neigbhour(10, 1)
    |"push-sum" ->
        echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)
    |_ ->
        printfn "not a proper algorithm"

// Creating the Imperfect 2D grid using maps
let imptwoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int // Rounding up to get a grid
    n <- sq * sq // Number of nodes in the grid
    let randomlist = new List<int>()
    
    match algorithm with // Spawning actors based on the algorithm
    |"gossip" ->
        spawnActorsForGossip()
    |"push-sum" -> 
        spawnActorsForPushSum()
        assignSumWeight(n)
    |_ -> 
        printfn "Enter Proper Algorithm gossip or push-sum"

    for i in [0 .. n-1] do // Creating the network
        let list = new List<int>()

        // Adding all the nodes that surround it as neighbours
        if i-1 >= 0 && i%sq <> 0 then 
            list.Add(i-1)
        if (i+1) % sq <> 0 then 
            list.Add(i+1)
        if i+4 < n then 
            list.Add(i+4)
        if i-4 >= 0 then 
            list.Add(i-4)
        let random = System.Random()
        let mutable number = random.Next(n-1) // Choosing a random neighbour
        while list.Contains(number) || number = i do
            number <- random.Next(0, n-1)

        list.Add(number)
        m <- m.Add(i, list) // Adding the actor number and list of neighbours
    
    let rnd = random.Next(0,n-1)

    time <- Stopwatch.StartNew()
    match algorithm with // Choosing algorithm and transmitting to a random neighbour
    |"gossip" -> 
        echoActors.[rnd] <! Neigbhour(10, 1)
    |"push-sum" ->
        echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)
    |_ ->
        printfn "not a proper algorithm"

// Master Actor
let master (mailbox: Actor<_>) = 
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        let mutable dummy = 0
        match msg with
        |Topo (topo, n) -> // Pattern matching for topology
            match topo with
            |"full" -> fullTopology(n)
            |"2D" -> twoDgrid(n)
            |"line" -> lineTopology(n)
            |"imp2D" -> imptwoDgrid(n)
            |_ -> printfn "improper topology select one of these full, 2D, line, imp2D"
        |Finished (node) -> // Checking which nodes have converged
            if not (finish.Contains(node)) then
                finish.Add(node)
            if finish.Count >= n-2 then // Checking condition
                flag <- true
        |_ -> printfn "unkown messageType recived"
        return! masterloop ()
    }      
    masterloop()

let main (args:string []) =
    n<-(int) args.[1] // Setting the value of number of nodes
    algorithm <- args.[3] // Setting the value for the algorithm
    t<-args.[2] // Setting the value value for the topology
    let mast = spawn system "master" master  // Spawning master actor  
    masterActor.Add(mast) // Adding master reference to the master actor system
    masterActor.[0] <! Topo (t, n) // Sending messages to the master actor about topology

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
