#./jobSubmitter.py cwb-qw160-df-s1 1 -n 5 -s 120
#./jobSubmitter.py cwb-qw160-df-s2 1 -n 5 -s 120
#./jobSubmitter.py cwb-qw160-df-s4 1 -n 5 -s 120
##./jobSubmitter.py cwb-qw160-df-s5 1 -n 5 -s 120
#./jobSubmitter.py cwb-qw160-df-s6 1 -n 5 -s 120
#./jobSubmitter.py cwb-qw160-df-s7 1 -n 5 -s 120
#./jobSubmitter.py cwb-qw160-df-s8 1 -n 5 -s 120
#./jobSubmitter.py cwb-qw160-df-s9 1 -n 5 -s 120
#./jobSubmitter.py cwb-qw160-df-s10 1 -n 5 -s 120
sleep 40m
for i in {6..10}
do 
	./jobSubmitter.py gov2-qw80-aol-df200-s${i} 1 -n 10 -s 180
done
