open System
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic
open Akka.FSharp
let mutable l=new List<int>()
l.Add(1)
l.Add(2)
l.Add(2)
l.Add(6)
l.Add(5)
l.Add(4)

printfn "%d" l.[l.Count-3]