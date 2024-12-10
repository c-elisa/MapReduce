package method

import(
	"slices"
	"os"
	"fmt"
	"mapreduce/utils"
)

type MapReduceHandler int

type MapRequest struct{
	Nums []int
}

type MapReply struct{
	Sorted []int
}

type ReduceRequest struct{
	Index int
	IntermediateResult []int
}

type ReduceReply struct{
	Message string
}

type MergeRequest struct{
	Index int
	Result []int
}

type MergeReply struct{
	Status int
	Message string
}

var MapResult []int
var ReduceResult []int

var MapDone bool
var ReduceDone bool
var reduceRequestCounter int
var reduceInput []int

var MergeRequestCounter int

func (t *MapReduceHandler) Map(request MapRequest, reply *MapReply) error{
	MapDone = false

	input := request.Nums
	fmt.Println("[WORKER] Received MAP request for: ", input)
	
	slices.Sort(input)
	MapResult = input
	reply.Sorted = input

	fmt.Println("[WORKER] Computed result for MAP request: ", input)

	MapDone = true

	return nil
}

func (t *MapReduceHandler) Reduce(request ReduceRequest, reply *ReduceReply) error{

	ReduceDone = false

	config := utils.ReadConfig()

	reduceRequestCounter++
	reduceInput = append(reduceInput, request.IntermediateResult...)
	
	if reduceRequestCounter==len(config.Map_nodes) {
		fmt.Println("[WORKER] Sequence to REDUCE", reduceInput)

		filename := config.Out_files[request.Index]

		slices.Sort(reduceInput)

		f, err := os.Create(filename)
		utils.CheckError(err)
		defer f.Close()

		for _, e := range reduceInput{
			fmt.Fprintln(f, e)
		}

		ReduceResult = reduceInput

		ReduceDone = true

		reduceInput = nil
		reduceRequestCounter = 0
	}

	reply.Message = "Request received correctly."

	return nil
}

func (t *MapReduceHandler) Merge(request MergeRequest, reply *MergeReply) error{

	MergeRequestCounter++
	fmt.Println("Received from REDUCE WORKER #", request.Index, " sequence: ", request.Result)

	reply.Status = 200
	reply.Message = "OK"

	return nil
}

