# To run: gnuplot -e "filename='foo'" render-graph.gnu
set datafile separator ","
set terminal svg size 2000 1000
set output (filename . ".svg")

plot (filename . ".csv") using 1:2 with lines
