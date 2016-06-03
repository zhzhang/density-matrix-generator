USAGE="usage: corpus_path targets [-h] [-s n] -- generator density matrices

where:
    -h  show this help text
    -d  dimension of matrices (default: soft cutoff, d = 0)
    -j  number of threads to use (default: 1)
    -j  number of runs to use, partitioning target words (default: 1)
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
    -d|--dim)
    DIM="$2"
    shift 2
    ;;
    -j|--jobs)
    NUM_THREADS="$2"
    shift 2
    ;;
    -o|--output)
    OUTPUT="$2"
    shift 2
    ;;
    -v|--vectors)
    VECTORS=1
    shift
    ;;
    -r|--runs)
    RUNS="$2"
    shift 2
    ;;
esac
done

java -cp build/libs/density-matrix-generator.jar dmatrix.SentenceDMatrixGenerator\
  $CORPUS_PATH $TARGETS $DIM $NUM_THREADS $VECTORS $OUTPUT $RUNS

