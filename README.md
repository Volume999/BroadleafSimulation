# Simulation of sequential vs asynchronous operations

## Running the simulation
1. Go to simulation folder
2. For latency, run in terminal:
    ```bash
    go test -bench=. -benchmem
    ```
   This benchmark runs for a non-specified amount of time (until the benchmark is stable). To run for a specific amount of time, run in terminal:
    ```bash
    go test -bench=. -benchmem -benchtime=100x
    ```
3. For throughput, run in terminal:
    ```bash
    go test -bench=. -benchmem -benchtime=5s
    ```
   The simulation will run for 5 seconds and output the results.
4. To run a complete benchmark:
    ```bash
    go test -bench=. -benchtime=5s -cpu=8 -timeout=0 -benchmem > outputFile.txt
    ```
   This will run the benchmark for 5 seconds, using 8 CPUs, with no timeout and memory allocation statistics, it will also output the results to a file called outputFile.txt.