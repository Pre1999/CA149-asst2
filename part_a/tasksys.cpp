#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::runthreads(IRunnable* runnable, int num_total_tasks) {
    int taskID = -1;
    while( this->current_task < num_total_tasks) {

        this->mutex_lock.lock();
        taskID = this->current_task;
        this->current_task += 1;
        this->mutex_lock.unlock();

        if(taskID < num_total_tasks) {
            runnable->runTask(taskID, num_total_tasks);
        }
    }

}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->current_task = 0;
    std::thread threadpool[num_threads];
    for (int i = 0; i < this->num_threads; i++) {
        threadpool[i] = std::thread([this, runnable, num_total_tasks]() {
            this->runthreads(runnable, num_total_tasks);
        });
    }
    for (int i = 0; i < this->num_threads; i++) {
        threadpool[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    for (int i = 0; i < this->num_threads; i++) {
        this->threadpool.emplace_back(&TaskSystemParallelThreadPoolSpinning::init_thread, this);
    }
}

void TaskSystemParallelThreadPoolSpinning::init_thread(){
    int last_task=-1;
    int my_thread_task=-1;
    while(!(this->thread_exit)){
        // printf("in thread func: loop start: next task %d | n num total tasks %d\n", next_task, num_total_tasks_spinning);
        
        // my_thread_task = next_task;
        // printf("value of cond: %d\n", my_thread_task < num_total_tasks);
        // printf("my thread task: %d\n", my_thread_task);
        
        // printf("lock acquired\n");
        this->task_increment_mutex.lock();
        if (this->next_task < this->num_total_tasks_spinning){
            // printf("in if statement\n");
            my_thread_task=this->next_task;
            this->next_task +=1;
            // printf("running threadID #%d\n", my_thread_task);
        }
        this->task_increment_mutex.unlock();
        
        if (my_thread_task < this->num_total_tasks_spinning && my_thread_task != last_task){
            last_task=my_thread_task;
            // printf("running taskID #%d\n", my_thread_task);
            this->task_queue->runTask(my_thread_task, this->num_total_tasks_spinning);
            this->task_increment_mutex.lock();
            this->finished_tasks += 1;
            // printf("Finished Tasks - #%d\n", this->finished_tasks);
            if(this->finished_tasks == this->num_total_tasks_spinning) {
                last_task=-1;
                my_thread_task=-1;
            }
            this->task_increment_mutex.unlock();
        } 

        // if ()
        // printf("loop end: thread_exit: %d\n", thread_exit);
    }
    // printf("Threads Exiting \n");
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->task_increment_mutex.lock();
    // printf("Here\n");
    this->thread_exit=true;
    this->task_increment_mutex.unlock();

    for (auto& thread : this->threadpool) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    // printf("Calling main function\n");
    this->task_increment_mutex.lock();
    this->finished_tasks = 0;
    this->task_queue = runnable;
    this->next_task = 0;
    this->num_total_tasks_spinning = num_total_tasks;
    this->task_increment_mutex.unlock();
    // printf("next task %d | num total tasks %d\n", next_task, num_total_tasks);
    // printf("-> %d | %d \n", this->finished_tasks, num_total_tasks);
    while(this->finished_tasks != (num_total_tasks)){
        // printf("Here\n");
        // printf("--> %d | %d \n", this->finished_tasks, num_total_tasks);
    }
    // printf("--> %d | %d \n", this->finished_tasks, num_total_tasks);
    // printf("------Here------------\n");
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
