package method

import(
	"slices"
	"os"
	"fmt"
	"mapreduce/utils"
	"net/rpc"
	"sync"
)

type MapHandler int

type MapRequest struct{
	Nums []int
}

type MapReply struct{
	Sorted []int
}

type ReduceRequest struct{
	Index int
}

type ReduceReply struct{
	AllSorted []int
}

type ShuffleRequest struct{
	Index int
}

type ShuffleReply struct{
	IntermediateResult []int
}

var MapResult []int

func (t *MapHandler) Map(request MapRequest, reply *MapReply) error{

	input := request.Nums
	fmt.Println("[WORKER] Received MAP request for: ", input)
	
	slices.Sort(input)
	MapResult = input
	reply.Sorted = input

	fmt.Println("[WORKER] Computed result for MAP request: ", input)

	return nil
}

func (t *MapHandler) Reduce(request ReduceRequest, reply *ReduceReply) error{

	index := request.Index
	fmt.Println("[WORKER", index, "Map result previously computed: ", MapResult)

	//read from config file
	config := utils.ReadConfig()

	// CONNECT TO WORKERS //

	addr := []string{}
	clients := make([]*rpc.Client, config.N_workers, config.N_workers)
	var err error

	// compute the addresses from config file
	for i:=0; i<config.N_workers; i++ {
		addr = append(addr, "localhost:" + config.Ports[i])
	}

	for i,a := range addr {
		clients[i], err = rpc.Dial("tcp", a)
		utils.CheckError(err)
		fmt.Println("[WORKER", request.Index, "] RPC server @", a, "dialed")
	}

	replies := make([]ShuffleReply, config.N_workers, config.N_workers)

	var wg sync.WaitGroup

	// send RPC calls to workers
	for i:=0; i<config.N_workers; i++ {
		
		args := ShuffleRequest{index}

		fmt.Println("[WORKER] Call to RPC server @", addr[i])

		wg.Add(1)

		//Sync call in separate gorouting
		go func(client *rpc.Client, args ShuffleRequest, reply *ShuffleReply){
			err = client.Call("Map.Shuffle", args, reply)
			utils.CheckError(err)
			wg.Done()
		}(clients[i], args, &replies[i])

	}

	//wait for all Map workers to finish executing
	wg.Wait()

	var input []int

	for i:=0; i<config.N_workers; i++ {
		input = append(input, replies[i].IntermediateResult...)
	}

	fmt.Println("[WORKER] Sequence to REDUCE", input)

	filename := config.Out_files[index]

	slices.Sort(input)
	reply.AllSorted = input

	f, err := os.Create(filename)
	utils.CheckError(err)
	defer f.Close()

	for _, e := range input{
		fmt.Fprintln(f, e)
	}

	return nil
}


func (t *MapHandler) Shuffle(request ShuffleRequest, reply *ShuffleReply) error{

	//index of the Reduce worker that sent the request
	index := request.Index
	n_workers := utils.Workers

	sampledInput := utils.GetSampledInput()

	var reduceSplit []int

	for _, e := range MapResult {
		if index == 0 {
			if e<sampledInput.SampledNums[index]{
				reduceSplit=append(reduceSplit, e)
			}
		}else if index == n_workers-1 {
			if e>sampledInput.SampledNums[index-1]{
				reduceSplit=append(reduceSplit, e)
			}
		}else{
			if e<sampledInput.SampledNums[index] && e>sampledInput.SampledNums[index-1]{
				reduceSplit=append(reduceSplit, e)
			}
		}
	}

	reply.IntermediateResult = reduceSplit

	return nil

}

