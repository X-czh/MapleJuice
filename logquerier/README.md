# Distributed Log Querier

## Design

Our distributed log querier system consists of a client sending out log queries and several servers where the log files locate. The client and the servers communicate through a TCP-based remote procedure call (RPC) interface. When the client issues a distributed log query, it makes remote procedure calls that send requests to each of the server, which calls a dispatch routine that performs grep on local log files and sends back the result.

To make the query fast, requests to each server are executed in parallel. The received results are also processed in parallel and stored in different files, one file per server. After all requests are finished, the client prints out a summary to the user.

The system is designed to be fault-tolerant. When the client fails to connect to a server (after a 5-second timeout), it will print a log, mark that server as crashed, and print the query results from the rest of the servers. If a server crashes after connecting with the client, the client will terminate the corresponding thread.

## Usage

### Server

```bash
go run server/logquery_server.go <port_number> <logfile_1> <logfile_2> ... <logfile_N>
```

You can register multiple log files on the server for querying. Note that the client program has no knowledge of the server's IP address and port number, and it is your responsibility to configure the client program with the correct server addresses.

### Client

You should put the servers' addresses in server_list.config (no newline in the end) like below:

```text
172.22.152.26:1234
172.22.154.22:1234
172.22.156.22:1234
172.22.152.27:1234
```

Then run:

```bash
go run client/logquery_client.go <grep_arg_1> <grep_arg_2> ... <grep_arg_N>
```

All grep options are supported, as all the arguments are forwarded to the system grep call internally. "-nH" option is automatically enabled for the grep call to display the file name and line number for each match. Matches from each server will be output to the corrresponding vm\[N\]output.txt file.

### Test

On each server you want to test on, run the server program in test mode:

```bash
go run server/logquery_server.go <port_number> --test
```

On your selected client machine, put the servers' addresses in server_list.config (no newline in the end), and run:

```bash
go run test/test.go <num_vms>
```

You can specify the number of virtual machines that you want to test with, as long as there are enough server address entries in the server_list.config file.
