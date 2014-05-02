

The input files File0..9 were generated with the hadoopTest.inputGenerator.MainInputGenerator.java class.

With that input, we run the hadoopTest.mapreduce.Main.java class, that use two Map-Reduce jobs.
The first one, MapCallers.java CombinerCallers.java. ReduceCallers.java group all the calls made by callers in the year 2012.
With out any restriction, the output was

A	118
B	113
C	106
D	105
E	88
F	91
G	105
H	95
I	122
J	102

Then we add the restriction to output only the ones with more than 100 calls (it make no sense for more than 20 calls due to the input distribution), and the output was 

A	118
B	113
C	106
D	105
G	105
I	122
J	102




The second Map-Reduce, MapSort.java, CombinerSort.java, ReduceSort.java, sort the output by descending number of calls, and with out any limitation the output was

122	I
118	A
113	B
106	C
105	D
105	G
102	J
95	H
91	F
88	E


Then we limit the number of output to 5, (to do that we also had to limit the number of Reducers to 1), and the output was 

122	I
118	A
113	B
106	C
105	D


