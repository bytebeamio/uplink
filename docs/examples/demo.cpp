#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <thread>
#include <sstream>
#include <vector>
#include <sys/time.h>
#include <netdb.h>
#include <string.h>
#include <mutex>
#include <fcntl.h>
#include <optional>

using namespace std;

optional<string> readUntilNewline(int socketFD) {
    std::vector<char> buffer;
    char c;
    while (true) {
        int res = read(socketFD, &c, 1);
        if (res == 0) {
            // socket is in blocking mode, read return value 0 means socket has closed
            return {};
        }
        if (c == '\n') {
            break;
        }
        buffer.push_back(c);
    }
    return string(buffer.begin(), buffer.end());
}

ssize_t writeToSocket(int socketFD, const string &data) {
    return send(socketFD, data.c_str(), data.length(), MSG_NOSIGNAL);
}

long current_time_millis() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  long long millis =
      ((long long)tv.tv_sec) * 1000 + ((long long)tv.tv_usec) / 1000;
  return millis;
}

int connectToUplink(int port) {
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo("127.0.0.1", std::to_string(port).c_str(), &hints, &res) != 0) {
        cout << "getaddrinfo failed for port " << port << endl;
        return 0;
    }

    int socketFD = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

    fcntl(socketFD, F_SETFL, O_NONBLOCK);

    fd_set fdset;
    struct timeval tv;
    FD_ZERO(&fdset);
    FD_SET(socketFD, &fdset);
    tv.tv_sec = 0;
    tv.tv_usec = 250000; // 250 ms connection timeout

    ::connect(socketFD, res->ai_addr, res->ai_addrlen);

    if (select(socketFD + 1, nullptr, &fdset, nullptr, &tv) == 1) {
        int so_error;
        socklen_t len = sizeof so_error;

        getsockopt(socketFD, SOL_SOCKET, SO_ERROR, &so_error, &len);

        if (so_error == 0) {
            freeaddrinfo(res);
            int flags = fcntl(socketFD, F_GETFL, 0);
            flags = flags & ~O_NONBLOCK;
            fcntl(socketFD, F_SETFL, flags);
            return socketFD;
        } else {
            cout << "getsockopt returned error " << so_error << endl;
            return 0;
        }
    }

    cout << "select failed for port " << port << endl;
    freeaddrinfo(res);
    return 0;
}

int main() {
    int socketFD = connectToUplink(8031);
    std::mutex clientLock;
    if (socketFD == 0) {
        cout << "couldn't connect" << endl;
        return 1;
    }

    std::thread actionResponder([&] () {
        while (true) {
            optional<string> lineO = readUntilNewline(socketFD);
            if (!lineO.has_value()) {
                break;
            }
            string line = lineO.value();
            clientLock.lock();
            cout << line << endl;
            // push action_status for this action
            clientLock.unlock();
        }
        cout << "stopping responder thread" << endl;
    });

    int sequence = 1;
    while (true) {
        sleep(1);
        ostringstream oss;
        oss << "{\"stream\": \"device_shadow\", \"sequence\": " << sequence++ << ", \"timestamp\": " << current_time_millis() << ", \"a\": 1, \"b\": 2 }" << endl;
        clientLock.lock();
        if (writeToSocket(socketFD, oss.str()) == -1) {
            break;
        }
        clientLock.unlock();
    }
    actionResponder.join();
    cout << "stopping pusher thread" << endl;
    return 0;
}