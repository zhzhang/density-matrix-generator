USAGE="usage: corpus_path targets [-h] [-s n] -- generator density matrices

where:
    -h  show this help text
    -j  number of threads to use (default: 1)
    -s  path to stopwords file"

CORPUS_PATH="$1"
TARGETS="$2"
shift 2
NUM_THREADS=1
STOPWORDS="stopwords.txt"

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
    shift # past argument
    ;;
    -s|--stopwords)
    STOPWORDS="$2"
    shift # past argument
    ;;
esac
shift
done

java -cp runtime/protobuf-java-2.6.1.jar -jar build/libs/density-matrix-generator.jar $CORPUS_PATH $TARGETS $NUM_THREADS $STOPWORDS

