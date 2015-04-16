-------------------------------------------------
 CS425 MP2: Chord P2P System
-------------------------------------------------

Ruichao Qiu (rqiu3@illinois.edu)
Hao Luo		(haoluo3@illinois.edu)

I. Usage 
	
	A Example usage:
		python run_chord.py -g file_name

		-g | output file storing result of all "show" commands

	B Chord command
		
		join <node_id>
			join a new node to the system
		find <node_id> <key_id>
			ask a node to locate a key
		leave <node_id>
			delete a node from the system
		show <node_id>
			show all keys stored in the node
		show all
			show all keys stored in all nodes, one line for each node

	C Output Information

	    The execution result of all commands will be outputted in Terminal
	    Only the result of "show" and "show all" operation will be outputted to the file


II. File list

	chord.py     					Chord class implementation
	run_chord.py                    Chord commmand line program
	README.md                       this file


III. System requirement

	Python version >= 2.7
