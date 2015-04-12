import random

def generate_rand_num_list(left_bound, right_bound, n, output_file):
	rand_list = set()
	while len(rand_list) < n:
		ri = random.randint(left_bound, right_bound)
		if not ri in rand_list:
			rand_list.add(ri)
	
	FILE = open(output_file, 'w')
	for n in rand_list:
		FILE.write("join {node_id}\n".format(node_id=n))
	FILE.close()
	return rand_list

def genearte_rand_num_lists(left_bound, right_bound, num_list, output_file):
	FILE = open(output_file, 'w')
	for n in num_list:
		print n
		print generate_rand_num_list(left_bound, right_bound, n)
		print "complete!"
	FILE.close()

#genearte_rand_num_lists(0, 255, [4, 8, 10, 20, 30])
print generate_rand_num_list(0, 255, 4, "input.txt")