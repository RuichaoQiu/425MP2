import argparse
from chord import CoordinatorThread

def main():
	arg_parser = argparse.ArgumentParser(description="Chord P2P System")
	arg_parser.add_argument('-g', \
							action='store', \
							dest='output_file', \
							default='', \
							help='file name for show-all output result', \
							required=False)

	args = arg_parser.parse_args()
	CThread = CoordinatorThread(args.output_file)

if __name__ == '__main__':
	main()