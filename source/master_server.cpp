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
#include "message.h"

using namespace std;

int accept_connection(int sockfd);
int listening_socket(int port);
int connect_socket(const char *hostname, const int port);

auto main(int argc, char *argv[]) -> int {
    int master_port = std::atoi(argv[2]); 
    int counter = 0;
    int socket, shard, server_return_port;
    map <int, int> server_array;
    socket = listening_socket(master_port);
    while(true) {

        // client or server connects
        int accept_socket =  accept_connection(socket);

        sockets::client_msg new_msg;
        auto [bytecount, buffer] = secure_recv(accept_socket);
        new_msg.ParseFromString(buffer.get());
        new_msg.PrintDebugString();

        std::string source_type = (std::string)new_msg.source_type();

        if (source_type == "server"){ 
            counter++;
            int32_t serverportno = new_msg.port();
            server_array[counter] = serverportno;


        } else if (source_type == "client"){ 
            
            int32_t k = new_msg.key();
            shard = (k % server_array.size()) + 1;
            server_return_port = server_array[shard];

            server::server_response rep;
            std::string buffer;
            rep.set_port(server_return_port);
            rep.SerializeToString(&buffer);
            auto buf = std::make_unique<char[]>(buffer.size() + length_size_field);
            construct_message(buf.get(), buffer.c_str(), buffer.size());
            secure_send(accept_socket, buf.get(), buffer.size() + length_size_field);
            buf.reset();
        }

    }
}



void error(const char *msg)
{
  perror(msg);
  exit(1);
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

int connect_socket(const char *hostname, const int port) {

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