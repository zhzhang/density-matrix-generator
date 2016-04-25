USAGE="usage: corpus_path targets [-h] [-s n] -- generator density matrices

where:
    -h  show this help text
    -d  dimension of matrices (default: 2000)
    -j  number of threads to use (default: 1)
    -o  output file (default: ./matrices.dat)
    -s  path to stopwords file"

CORPUS_PATH="$1"
TARGETS="$2"
shift 2
NUM_THREADS=1
DIM=2000
STOPWORDS="stopwords.txt"
OUTPUT="matrices.dat"

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
    shift # past argument
    ;;
    -j|--jobs)
    NUM_THREADS="$2"
    shift # past argument
    ;;
    -o|--output)
    OUTPUT="$2"
    shift # past argument
    ;;
    -s|--stopwords)
    STOPWORDS="$2"
    shift # past argument
    ;;
esac
shift
done

java -jar build/libs/density-matrix-generator.jar \
  $CORPUS_PATH $TARGETS $DIM $NUM_THREADS $STOPWORDS $OUTPUT

