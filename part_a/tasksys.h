#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <mutex>
#include <atomic>
#include <cstdio>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        int num_threads;
        int current_task = 0;
        std::mutex mutex_lock;
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        void runthreads(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        int getNumThreads(){
            return num_threads;
        }
        void setNumThreads(int incoming_value){
            num_threads = incoming_value;
        }
    private:
        int num_threads;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        int num_threads=0;
        int next_task=0;
        int num_total_tasks_spinning=0;
        std::atomic<int> finished_tasks{0};
        std::mutex task_increment_mutex;
        std::vector<std::thread> threadpool;
        IRunnable* task_queue;
        std::atomic<bool> thread_exit{false};
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        void init_thread();
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        int getNumThreads(){
            return num_threads;
        }
        void setNumThreads(int incoming_value){
            num_threads = incoming_value;
        }
        int countZeros(int arr[], int size) {
            int count = 0;
            for (int i = 0; i < size; i++) {
                if (arr[i] == 0) {
                    count++;
                }
            }
            return count;
        }
    private:
        int num_threads;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        int num_threads;
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void setNumThreads(int incoming_value){
            num_threads = incoming_value;
        }
        void execute_task(IRunnable* runnable, int num_total_tasks, int* next_task, std::mutex* task_mutex){
            int my_thread_task=0;
            bool stop=false;
            while(!stop){
                task_mutex->lock();
                if (*next_task < num_total_tasks){
                    my_thread_task=*next_task;
                    *next_task +=1;
                    
                }
                else {
                    stop=true;
                }
                task_mutex->unlock();
                if (! stop){
                    // printf("running task %d\n", my_thread_task);
                    runnable->runTask(my_thread_task, num_total_tasks);
                }
                
            }
        }
};

#endif
