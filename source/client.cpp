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

using namespace std;

auto main(int argc, char *argv[]) -> int {
  
  bool direct;
  int port = std::atoi(argv[2]);
  std::string operation = argv[4];
  int32_t key = std::atoi(argv[6]);
  std::string value = argv[8];
  int master_port = std::atoi(argv[10]);
  int serv_port;

  if (std::atoi(argv[12]) == 0){
    int shard_socket = connect_socket(master_port);
    sockets::client_msg message;
    std::string buffer;
    message.set_key(key);
    message.set_source_type("client");
    message.SerializeToString(&buffer);
    auto buf = std::make_unique<char[]>(buffer.size() + length_size_field);
    construct_message(buf.get(), buffer.c_str(), buffer.size());
    secure_send(shard_socket, buf.get(), buffer.size() + length_size_field);
    buf.reset();

    server::server_response new_msg;
    auto [bytecount, buffer1] = secure_recv(shard_socket);
    new_msg.ParseFromString(buffer1.get());
    new_msg.PrintDebugString();
    close (shard_socket);

    serv_port = new_msg.port();
  }
  else if (std::atoi(argv[12]) == 1){
    serv_port = port;
  }
  int serv_sock = connect_socket(serv_port);
  


  if (operation == "PUT") {
    sockets::client_msg message1;
    std::cout << "key-1: " << key << std::endl;
    std::cout << "val-1: " << value << std::endl;
    message1.set_operation(sockets::client_msg:: PUT);
    message1.set_value(value);
    message1.set_key(key);
    std::cout << "here " << std::endl;

    message1.PrintDebugString();
    std::string buffer;
    message1.SerializeToString(&buffer);
    auto buf = std::make_unique<char[]>(buffer.size() + length_size_field);
    construct_message(buf.get(), buffer.c_str(), buffer.size());
    secure_send(serv_sock, buf.get(), buffer.size() + length_size_field);
    buf.reset();

    cout << "now receiving" << endl;
    

    sockets::client_msg serv_rep;
    auto [bytecount, buffer3] = secure_recv(serv_sock);
    serv_rep.ParseFromString(buffer3.get());
    std::cout << "server response: " << key << std::endl;
    std::cout << "server response val: " << value << std::endl;

    serv_rep.PrintDebugString();

    close (serv_sock);
    if (bytecount <= 0) {
      cout << "shard receive failed" << endl;
    }

    if (serv_rep.success()) {
        exit(0);
    } else {
        exit(2);
    }

  }
  else if (operation == "GET"){
    sockets::client_msg message1;
    
    cout << "get" << endl;
    message1.set_operation(sockets::client_msg:: GET);
    message1.set_value(value);
    message1.set_key(key);
    message1.PrintDebugString();

    std::string buffer2;
    message1.SerializeToString(&buffer2);
    auto buf = std::make_unique<char[]>(buffer2.size() + length_size_field);
    construct_message(buf.get(), buffer2.c_str(), buffer2.size());
    secure_send(serv_sock, buf.get(), buffer2.size() + length_size_field);
    buf.reset();

    cout << "int1" << endl;
    sockets::client_msg serv_rep;
    auto [bytecount, buffer3] = secure_recv(serv_sock);
    serv_rep.ParseFromString(buffer3.get());
    serv_rep.PrintDebugString();

    close (serv_sock);
    if (bytecount <= 0) {
      cout << "shard receive failed" << endl;

    }

    if (serv_rep.success()) {
          exit(0);
    } else {
          exit(2);
    }

  }

  return 0;
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