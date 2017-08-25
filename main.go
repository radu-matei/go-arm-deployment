package main

import (
	"os"
	"flag"
	"fmt"
)

func main() {
    flag.Parse()
    args := flag.Args()
    if len(args) < 1 {
        fmt.Printf("Please specify a command!\n\n")
        flag.Usage()
        os.Exit(1)
    }

    switch args[0] {
        case "push":
            push(args[1:])
        case "run": 
            run(args[1:])

    default:
        fmt.Printf("%q is not a valid command \n\n", args[0])
        flag.Usage()
        os.Exit(1)
    }
}