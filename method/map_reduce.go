package method

import(
	"slices"
	"os"
	"mapreduce/utils"
	"fmt"
	"log"
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
	log.Printf("Received MAP request for: %v", input)
	
	slices.Sort(input)
	MapResult = input
	reply.Sorted = input

	log.Printf("Computed result for MAP request: %v", input)

	MapDone = true

	return nil
}

func (t *MapReduceHandler) Reduce(request ReduceRequest, reply *ReduceReply) error{

	ReduceDone = false

	config := utils.ReadConfig()

	reduceRequestCounter++
	reduceInput = append(reduceInput, request.IntermediateResult...)
	
	// wait for all Mappers to finish
	
	if reduceRequestCounter==len(config.Map_nodes) {
		log.Printf("Sequence to REDUCE: %v", reduceInput)

		filename := "inout_files/" + config.Out_files[request.Index]

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
	log.Printf("Received from REDUCE WORKER #%d sequence: %v", request.Index, request.Result)

	reply.Status = 200
	reply.Message = "OK"

	return nil
}

