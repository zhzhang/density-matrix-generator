# density-matrix-generator
Generating density matrix representations of words using Java.

To build the package, install gradle and run
```
~$ gradle build
```
The build may error if it is unable to remove the temporary test directories. If this happens, the compiled jar is still usable.

The compiled jar may be used directly in the project directory using the three provided generation scripts.
Run
```
~$ ./generate_sent.sh -h
```
for example, to view the settings and parameters.

The input data expected is currently very specialized.
The sentence co-occurrence and embedding matrix generator expects lemmatized Wikipedia dumps as input, with one sentence per line.
The embedding matrix generator also requires embedding word vectors in the text-based format, which is standard output of Word2Vec.
The dependency matrix generator expects collapsed dependencies in the Universal Stanford Dependency format, serialized using MessagePack.
Target words may be passed as a text file, with one word per line.
