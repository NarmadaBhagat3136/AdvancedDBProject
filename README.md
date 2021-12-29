# repcrec_adbms

repcrec is Replicated Concurrency Control and Recovery (RepCRec) System. 

**** Part 1 - Running the code using Reprozip ****

Run the reprounzip commands on the repcrec.rpz file. 
1. Run command: reprounzip directory setup repcrec.rpz unpacked
2. Run command: reprounzip directory run unpacked
3. The above will generate the respective outputs for the two runs:
	a. run0 is python3 dbms.py --input sample_tests_i_1 --output sample_tests_o_1
	b. run1 is python3 dbms.py --input batch_input --output batch_output --batch True
4. To verify/read the output files, navigate to the unpacked/root/home/arm994/adbms_2/repcrec directory. Here, you will find the output files.

**** Part 2 - Run the code as a Python application ****

To run the code, use the commands as below.
usage: dbms.py [-h] [--input INPUT] [--output OUTPUT] [--batch BATCH] [--v V]
               [--dumpAtEnd DUMPATEND]

optional arguments:
  -h, --help            show this help message and exit
  --input INPUT         Input file name
  --output OUTPUT       Output file name
  --batch BATCH         Is batch mode? True/False, Default: False
  --v V                 Verbose Mode (print only dump or all statements):
                        True/False, Default: False
  --dumpAtEnd DUMPATEND
                        Always dump() on completion of test case: True/False, Default: False

Examples:
1. Single test case [--batch=False]
python3 dbms.py --input sample_tests_i_1 --output sample_tests_o_1 

2. Batch Mode [--batch=True]
python3 dbms.py --input batch_input --output batch_output --batch True