#sleep 3h
#./jobSubmitter.py cachetest-new-taily 1 -n 12 -s 90
for i in {0..9}
    do condor_submit taily_${i}.job
    sleep 90s
done
