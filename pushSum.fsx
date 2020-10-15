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
open System.Diagnostics

// let mutable time =  System.Diagnostics.Stopwatch()

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
    |   Receive of float * float * int
    |   Selfsender of float * float * int

//Creating Actor System
//let mutable echoActors=new List<IActorRef>()
let mutable echoActorsPS=new List<IActorRef>()

//Creating Master Actors
//let mutable masterActor=new List<IActorRef>()
let mutable masterActorPS=new List<IActorRef>()

let mutable m = Map.empty<int, List<int>>

let mutable sumMap = Map.empty<int, float>
let mutable weightMap = Map.empty<int, float>
// let mutable listRatio = new List<float>()
let mutable time =  System.Diagnostics.Stopwatch()
let mutable ak=0

let mutable finish = new List<int>()

let random = Random()
let mutable rnd=0
let mutable waittime = 0
let mutable total = 0
let wait() = 
    for i = 1 to 100000 do 
        waittime <- waittime+1 
// Actor model for Push Sum Algorithm
let assignSumWeight(n) = 
    for i = 0 to n-1 do 
        sumMap <- sumMap.Add(i,float(i))
        weightMap <- weightMap.Add(i, 1.0)

let echoPS (mailbox:Actor<_>) = 
    // printfn "Entered actor with random : %d" rnd
    let mutable count=0
    // let mutable s=0//Get actor number s is an actor number
    // let mutable a=0
    // let mutable gee=0
    let mutable nodeCount = 0 
    let mutable ratioList=new List<float>()
    let mutable weight=1
    // let mutable ratio=float(s)
    
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
                    // printfn "length of sequenc %d" m.[num].Count
            if finish.Count >= n-2 then
                flag <- true

            let mutable kee = m.[nodeNumber].[rand]
            // mailbox.Self <! Self(num, 0)
            if not (finish.Contains(nodeNumber)) || finish.Count >= n-2 then
                mailbox.Self <! Selfsender(s_rec, w_rec, nodeNumber)
            wait()
            if not (finish.Contains(kee)) then
                echoActorsPS.[kee] <! Receive(totalSum, totalWeight, kee)
            else 
                while finish.Contains(kee) do
                    kee <- random.Next(0, n-1)
                echoActorsPS.[kee] <! Selfsender(s_rec, w_rec, kee)
        | Selfsender(s_rec, w_rec, nodeNumber) ->
            //printfn "here ratioList length is %d" ratioList.Count
            if ratioList.Count = 1 then 
                total <- total + 1
                //printfn "node %d started and total is %d" nodeNumber total

            if(ratioList.Count>=3 && ((s_rec/w_rec)-ratioList.[ratioList.Count-1])<(10.0**(-10.0)) && ((s_rec/w_rec)-ratioList.[ratioList.Count-2])<(10.0**(-10.0)) && ((s_rec/w_rec)-ratioList.[ratioList.Count-3])<(10.0**(-10.0)) && not (finish.Contains(nodeNumber))) then //Stopping Condition
                let mutable t = s_rec/ w_rec
                // printfn "ratio is %f of node :%d" t nodeNumber
                masterActorPS.[0]<! Finished(nodeNumber)

            wait()
            if not (finish.Contains(nodeNumber)) then
                mailbox.Self <! Receive(s_rec, w_rec, nodeNumber)


        return! loop ()
        
    }
    loop ()

let lineTopology(k:int) = 

    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties echoPS
        echoActorsPS.Add(actor) 
    assignSumWeight(n)
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
    //     printfn "%d:%A" i m.[i]
    let rnd = random.Next(0,n-1)
    printfn "random %d " rnd
    // time <- System.DateTime.Now.Millisecond
    // printfn "time %u" time   
    //time <- Stopwatch.StartNew()
    time <- Stopwatch.StartNew()
    echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)

    //echoActorsPS.[rnd] <! Neigbhour(9, 1)

let twoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int
    n <- sq * sq
    assignSumWeight(n)
    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties echoPS
        echoActorsPS.Add(actor) 
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
    //     printfn "%d:%A" i m.[i]
    let rnd = random.Next(0,n-1)
    // printfn "random %d " rnd
    // let mutable start = n / 2
    // printfn "sumne %d" start
    // time <- System.DateTime.Now.Millisecond
    // time <- Stopwatch.StartNew()
    time <- Stopwatch.StartNew()
    echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)

let imptwoDgrid(k:int) = 
    let sq = ceil(sqrt(float(k))) |> int
    n <- sq * sq
    assignSumWeight(n)
    let randomlist = new List<int>()
    for i in [0 .. n-1] do
        let properties = string(i)
        let actor = spawn system properties echoPS
        echoActorsPS.Add(actor) 
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
    //     printfn "%d:%A" i m.[i]
    let rnd = random.Next(0,n-1)
    // printfn "random %d " rnd
    // let mutable start = n / 2
    // printfn "sumne %d" start
    // time <- System.DateTime.Now.Second
    // time <- Stopwatch.StartNew()
    time <- Stopwatch.StartNew()
    echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd)


let fullTopology(n: int)=
    for i in [0 .. n] do
        let properties = string(i)
        let actor = spawn system properties echoPS
        echoActorsPS.Add(actor) 
    assignSumWeight(n)
    for i in [0 .. n] do
        let l = new List<int>()
        for j in [0 .. n] do
            if j <> i then 
                l.Add(j)
        m<-m.Add(i,l)
        // printfn "%A" m
    rnd <- random.Next(0,n-1)
    // time <- Stopwatch.StartNew()
    printfn "Random chosen: %d" rnd
    //echoActors.[rnd] <!rnd, 0)
    time <- Stopwatch.StartNew()
    echoActorsPS.[rnd] <! Receive(sumMap.[rnd],weightMap.[rnd],rnd) //First call


let masterPS(mailbox: Actor<_>)= 
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        // printfn "%A" n
        let mutable dummy = 0
        match msg with
        |Topo (topo, n) ->
                //lineTopology(n)
                twoDgrid(n)
                //imptwoDgrid(n)
                //fullTopology(n)
                // echoActorsPS.[0] <! Self (0, 1) 
        |Finished (node) ->
                if not (finish.Contains(node)) then
                    finish.Add(node)
                    //printfn "node %d finished and total is %d" node finish.Count
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

    time.Stop()
    printfn "Time taken for %A topology to converge is %f" t time.Elapsed.TotalMilliseconds    
    0

let args = fsi.CommandLineArgs 
match args.Length with //Checking number of parameters
    | 4 -> main args    
    | _ ->  failwith "You need to pass 3 parameters!"
