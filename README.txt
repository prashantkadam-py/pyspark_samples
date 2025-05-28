

JOBS --> Stage --> task1, task2
     --> Stage --> task3, task4


Driver --> executor1 : core1, core2
       --> executor2 : core3, core4


each core can perform local count operation
then 1 core will collect local count from other cores and calculate global count, 
to calculate local count data shuffling operation is conducted.

