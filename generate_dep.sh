USAGE="usage: corpus_path targets [-h] [-s n] -- generate density matrices

where:
    -h  show this help text
    -d  number of context words to use (default: soft cutoff, d = 0)
    -j  number of threads to use (default: 1)
    -r  number of runs to use, partitioning target words (default: 1)
    -o  output file (default: matrices/)
    -v  generate and output vector representation"

CORPUS_PATH="$1"
TARGETS="$2"
shift 2
NUM_THREADS=1
DIM=0
OUTPUT="matrices"
VECTORS=0
RUNS=1

while [[ $# > 0 ]]
do
key="$1"
case $key in
    -h|--help)
    echo "$USAGE"
    exit
    ;;
    -j|--jobs)
    NUM_THREADS="$2"
    shift 2
    ;;
    -d|--dimension)
    DIM="$2"
    shift 2
    ;;
    -o|--output)
    OUTPUT="$2"
    shift 2
    ;;
    -r|--runs)
    RUNS="$2"
    shift 2
    ;;
    -v|--vectors)
    VECTORS=1
    shift
    ;;
esac
done

java -cp build/libs/density-matrix-generator.jar dmatrix.DependencyDMatrixGenerator\
  $CORPUS_PATH $TARGETS $DIM $NUM_THREADS $VECTORS $OUTPUT $RUNS

