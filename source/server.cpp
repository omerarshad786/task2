#include <atomic>
#include <iostream>
#include <string>
#include <iostream>
#include <string>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <limits.h>
#include "shared.h"
#include "message.h"
#include <pthread.h>
#include <assert.h>
#include "rocksdb/db.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdbool.h> 
#include <limits.h>

int connect_socket(const int port);
void error(const char *msg);
int listening_socket(int port) ;
int accept_connection(int sockfd) ;
int recv_request(int port, sockets::client_msg *new_msg);
void handleConn(int client_socket);

rocksdb::DB* db;

using namespace std;

auto main(int argc, char *argv[]) -> int {
    int server_port = std::atoi(argv[2]); 
    int master_port = std::atoi(argv[4]); 
    cout << server_port << master_port << endl;
    int socket = connect_socket(master_port);
    sockets::client_msg message;
    std::string buffer;
    message.set_port(server_port);
    message.set_source_type("server");
    message.SerializeToString(&buffer);
    auto buf = std::make_unique<char[]>(buffer.size() + length_size_field);
    construct_message(buf.get(), buffer.c_str(), buffer.size());
    secure_send(socket, buf.get(), buffer.size() + length_size_field);
    buf.reset();


    rocksdb::Options optionsh;
	optionsh.create_if_missing = true;
	rocksdb::Status status = rocksdb::DB::Open(optionsh, "./testdb_"+std::to_string(server_port), &db);
    assert(status.ok());

    int listen_sock = listening_socket(server_port);
    
    while(true){
        int client_socket = accept_connection(listen_sock);
        sockets::client_msg new_msg1;

        handleConn(client_socket);

        // if(recv_request(client_socket, &new_msg1)) { 
        //     cout << "here" << endl;
            
        //     new_msg1.PrintDebugString(); 

        //     cout << "new here" << endl;
        //     cout << new_msg1.operation() << endl;
        //     if (new_msg1.operation() == sockets::client_msg:: GET) {
        //         std::cout << "GET1" << std::endl;
        //         bool success;
        //         int32_t key = new_msg1.key();
        //         std::cout << "key: " << key << std::endl;
        //         std::string val;
        //         cout << "getting value" << endl;
        //         rocksdb::Status s = db->Get(rocksdb::ReadOptions(), std::to_string(key), &val);
                
        //         std::cout << "value: " << val << std::endl;


        //         if (s.ok()) { 
        //             success = true; 
        //             cout << "here" << endl;
        //         }
        //         else { 
        //             success = false; 
        //             cout << "its here" << endl;

        //         }
                
        //         sockets::client_msg rep;
        //         rep.set_success(success);
        //         std::string buffer1;
        //         rep.SerializeToString(&buffer1);
        //         auto buf = std::make_unique<char[]>(buffer1.size() + length_size_field);
        //         construct_message(buf.get(), buffer1.c_str(), buffer1.size());
        //         secure_send(client_socket, buf.get(), buffer1.size() + length_size_field);
        //         buf.reset();
        //     }
        //     else if (new_msg1.operation() == sockets::client_msg:: PUT) {
        //         std::cout << "PUT1" << std::endl;
        //         bool success;
        //         int32_t key = new_msg1.key();
        //         std::string val = new_msg1.value();
        //         std::cout << "key: " << key << std::endl;
        //         std::cout << "value: " << val << std::endl;

        //         sockets::client_msg rep;
        //         rocksdb::Status s = db->Put(rocksdb::WriteOptions(), std::to_string(key), val);
               
        //         if (s.ok()) { 
        //             success = true; 
        //             std::cout << "here at put"<< std::endl;

        //         }
        //         else { 
        //             success = false; 
        //             std::cout << "here at put failed"<< std::endl;
                    
        //         }

                
        //         rep.set_success(success);               
        //         std::string buffer1;
        //         rep.SerializeToString(&buffer1);
        //         auto buf = std::make_unique<char[]>(buffer1.size() + length_size_field);
        //         construct_message(buf.get(), buffer1.c_str(), buffer1.size());
        //         secure_send(client_socket, buf.get(), buffer1.size() + length_size_field);
        //         buf.reset();
        //     }
  		    
        // }
        // else {
        //     cout << "recv+fuc" << endl;
        //     break;
        // }	
	}

    
}

void handleConn(int client_socket){
    sockets::client_msg message;
    auto [bytes, buffer] = secure_recv(client_socket);

    auto payload_sz = bytes;
    std::string tmp(buffer.get(), payload_sz);
    message.ParseFromString(tmp);

    int32_t key;
    int32_t success;
    std::string value;

    key = message.key();
    value = message.value();

    std::cout << "key: " << key << std::endl;
    std::cout << "value: " << value << std::endl;

    if (message.operation() == sockets::client_msg::GET){
        bool success;
        int32_t key = message.key();
        std::cout << "key: " << key << std::endl;
        std::string val;
        cout << "getting value" << endl;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), std::to_string(key), &val);
        
        std::cout << "value: " << val << std::endl;


        if (s.ok()) { 
            success = true; 
            cout << "here" << endl;
        }
        else { 
            success = false; 
            cout << "its here" << endl;

        }
        
        sockets::client_msg rep;
        rep.set_success(success);
        std::string buffer1;
        rep.SerializeToString(&buffer1);
        auto buf = std::make_unique<char[]>(buffer1.size() + length_size_field);
        construct_message(buf.get(), buffer1.c_str(), buffer1.size());
        secure_send(client_socket, buf.get(), buffer1.size() + length_size_field);
        buf.reset();
    }
    else{
        bool success;
        int32_t key = message.key();
        std::string val = message.value();
        std::cout << "key: " << key << std::endl;
        std::cout << "value: " << val << std::endl;

        
        rocksdb::Status s = db->Put(rocksdb::WriteOptions(), std::to_string(key), val);
        
        if (s.ok()) { 
            success = true; 
            std::cout << "here at put"<< std::endl;

        }
        else { 
            success = false; 
            std::cout << "here at put failed"<< std::endl;
            
        }

        sockets::client_msg rep;
        rep.set_success(success);               
        std::string buffer1;
        rep.SerializeToString(&buffer1);
        auto buf = std::make_unique<char[]>(buffer1.size() + length_size_field);
        construct_message(buf.get(), buffer1.c_str(), buffer1.size());
        secure_send(client_socket, buf.get(), buffer1.size() + length_size_field);
        buf.reset();
    }
    
}



int recv_request(int port, sockets::client_msg *new_msg) {
    std::cout << "startss1" << std::endl;
    
    auto [new_bytecount,new_buffer] = secure_recv(port);

    if (new_bytecount == 0) {
        return 0;
    } else {
        new_msg->ParseFromString(new_buffer.get());
        std::cout << "startss4" << std::endl;
        new_msg->PrintDebugString();
        std::cout << "startss5" << std::endl;

        return 1;
    }
}


void error(const char *msg)
{
  perror(msg);
  exit(1);
}

int connect_socket(const int port) {

    int sockfd;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr;

    if (sockfd < 0) {
        error("ERROR connecting/opening socket");
        return -1;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        error("Invalid address/ Address not supported");
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        error("connection failed");
        return -1;
    }

    return sockfd;

}

int listening_socket(int port) {
    struct sockaddr_in serv_addr;
    int sockfd;
    //char buffer[1024];

    // socket file descriptor
    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd < 0) {
        error("ERROR opening socket");
        return -1;
    }

    if (sockfd == 0) {
        error("socket failed");
        return -1;
    }

    int opt = 1;
    // attaching socket to port 8080 forcefully
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        error("setsockopt");
        return -1;
    }

    //bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    // Forcefully attaching socket to the port 8080
    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
        error("ERROR on binding");
        return -1;
    }

    if (listen(sockfd, 3) < 0) {
        error("error listening");
        return -1;
    }

    //std::cout << "socket listening done func" << std::endl;
    return sockfd;
    
}

int accept_connection(int sockfd) {

    struct sockaddr_in cli_addr;
    int clilen = sizeof(cli_addr);
    int newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, (socklen_t*) &clilen);
    if (newsockfd < 0) {
        error("ERROR on accept");
        return -1;
    }
    //std::cout << "socket accept done func" << std::endl;
    return newsockfd;
}