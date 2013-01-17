//
//  Server-Client queue device
//  A simple Client Server Queueing system
//
// David Seward dave@zucchi.co.uk

#include "zmq/zhelpers.hpp"

#include <queue>

using namespace std;

int main (int argc, char *argv[])
{
	cout << "Started" << endl;
	s_version_assert (2, 1);
	//  Prepare our context and sockets
	zmq::context_t context(1);
	zmq::socket_t frontend (context, ZMQ_ROUTER);
	zmq::socket_t backend (context, ZMQ_ROUTER);
	frontend.bind("tcp://*:5555");    //  For clients
	backend.bind("tcp://*:5556");     //  For workers

	queue<string> queue;

	while (1) {

		//  Initialize poll set
		zmq::pollitem_t items [] = {
			//  Always poll for worker activity on backend
			{ backend,  0, ZMQ_POLLIN, 0 },
			//  Poll front-end only if we have available workers
			{ frontend, 0, ZMQ_POLLIN, 0 }
		};
		
		zmq::poll (items, 2, -1); //this seems to detect polls from both backend and frontend
		// zmq::poll (items, 1, -1);

		//  Handle worker activity on backend
		if (items [0].revents & ZMQ_POLLIN) {

			//get message from worker
			zmq::message_t message(0);
			string worker_addr = s_recv (backend);
			{
				string empty = s_recv (backend);
				assert (empty.size() == 0);
			}

			string msg = s_recv (backend); 
			//cout << "worker addr: " << worker_addr << endl;
			//cout << "worker reply: " << msg << endl;

			string queueMsg;
			if(msg == "ready") {
				if(queue.size() > 0) {
					queueMsg = queue.front();
					queue.pop();
				}
				else
					queueMsg = "empty";

				//cout << "queue msg: " << queueMsg << endl;
				//send reply to worker with contents of queue
				s_sendmore (backend, worker_addr);
				s_sendmore (backend, "");
				s_send     (backend, queueMsg); 
			}
		}

		//receive msg from client
		if (items [1].revents & ZMQ_POLLIN) {

			//get message from client
			zmq::message_t message(0);
			string client_addr = s_recv (frontend);
			{
				string empty = s_recv (frontend);
				assert (empty.size() == 0);
			}

			string msg = s_recv (frontend); 

			if(msg == "purge") {
				while (!queue.empty())
					queue.pop();
				//cout << "queuesize: " << queue.size() << endl;
			}
			else 
				queue.push(msg);

			//cout << "client addr: " << client_addr << endl;
			//cout << "client reply: " << msg << endl;

			//send reply to client so say that message received correctly
			s_sendmore (frontend, client_addr);
			s_sendmore (frontend, "");
			s_send     (frontend, msg); 

		}
	}
	sleep (1);
	return 0;
}