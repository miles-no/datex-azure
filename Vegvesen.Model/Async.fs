namespace Vegvesen.Model

module Async =

    let map f workflow = async {
        let! res = workflow
        return f res }

    let iter f workflow = async {
        let! res = workflow
        do f res }

    let concat workflow = 
        Async.Parallel workflow
        |> map Seq.concat

