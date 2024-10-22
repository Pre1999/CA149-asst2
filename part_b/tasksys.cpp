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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */
Tasks::Tasks() {
    finished_tasks_ = -1;
    next_task = -1;
    num_total_tasks_ = -1;
    runnable_ = nullptr;
    status_=1;
}

Tasks::~Tasks() {
}

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
    mutex_ = new std::mutex();
    finishedMutex_ = new std::mutex();
    finished_ = new std::condition_variable();
    killed_ = false;
    num_runs_started=0;
    num_runs_finished=0;
    num_threads_ = num_threads;
    thread_pool_ = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::sleepingThread, this, i);
    }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    printf("Destructor called \n");
    mutex_->lock();
    killed_ = true;
    mutex_->unlock();

    //wake up threads to see that killed_ is true
    finishedMutex_->lock();
    finishedMutex_->unlock();
    finished_->notify_all();

    for (int i = 0; i < num_threads_; i++)
    thread_pool_[i].join();
    printf("threads have been joined");
    delete[] thread_pool_;
    delete tasks;
    delete mutex_;
    delete finishedMutex_;
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
    //increment total active runs 
    int taskID=num_runs_started;
    num_runs_started++;
    
    tasks = new Tasks();
    mutex_->lock();
    tasks->finished_tasks_ = 0;
    tasks->next_task = 0;
    tasks->num_total_tasks_ = num_total_tasks;
    tasks->runnable_ = runnable;
    tasks->status_ = 0;
    tasks->deps = deps;
    bulk_task_launches[taskID] = tasks;
    for (auto& pair: bulk_task_launches){
        printf("taskID %d", taskID);
        for (auto& dep_:pair.second->deps){
            printf(" | dep %d", dep_);
        }
        printf("\n");
    }
    mutex_->unlock();

    //wake up threads to do work
    printf("waking threads to do work\n");
    finishedMutex_->lock();
    finishedMutex_->unlock();
    finished_->notify_all();

    printf("exiting runAsyncWithDeps\n");
    return taskID;
}
void TaskSystemParallelThreadPoolSleeping::sleepingThread(int threadID) {
    int taskID;
    int total;
    printf("threadID %d entering\n", threadID);
    while (!killed_)
    {
        for (auto& pair: bulk_task_launches){
            finishedMutex_->lock();
            if (pair.second->status_==1) {
                continue;
            }
            finishedMutex_->unlock();
            // printf("trying to run taskID: %d\n", pair.first);
            //check for any dependencies on the task to exec
            for (auto& depend_:pair.second->deps){
                printf("checking for dependency on taskID: %d\n", depend_);
                finishedMutex_->lock();
                if (! bulk_task_launches[depend_]->status_ || pair.second->status_==1){ //status 0 means not ready,not finished,  1 is ready/finished
                finishedMutex_->unlock();
                    continue;
                }
            }
            
            
            mutex_->lock();
            // if (killed_) break;
            std::unique_lock<std::mutex> lk(*(finishedMutex_));
            finished_->wait(lk);

            total = pair.second->num_total_tasks_;
            taskID = pair.second->next_task;
            if (taskID < total) pair.second->next_task++;
            mutex_->unlock();
            if (taskID < total) {
                pair.second->runnable_->runTask(taskID, total);
                mutex_->lock();
                pair.second->finished_tasks_++;
                if (pair.second->finished_tasks_ == total) {
                    mutex_->unlock();

                    finishedMutex_->lock();
                    num_runs_finished++;
                    printf("num runs finished: %d threadID %d\n", num_runs_finished, threadID);
                    killed_=true;
                    pair.second->status_=1; //set status of finished task to 1 meaning ready
                    finishedMutex_->unlock();
                } else {
                    mutex_->unlock();
                }
            }
        }
    }

    printf("threadID %d exiting\n", threadID);
    
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    while(num_runs_finished < num_runs_started){
        // printf("current runs finished: %d\n", num_runs_finished);
    }
    printf("Exiting sync \n");
    return;
}
