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

Tasks::Tasks() {
    mutex_ = new std::mutex();
    finishedMutex_ = new std::mutex();
    finished_ = new std::condition_variable();
    finished_tasks_ = -1;
    next_task = -1;
    num_total_tasks_ = -1;
    runnable_ = nullptr;
}

Tasks::~Tasks() {
    delete mutex_;
    delete finished_;
    delete finishedMutex_;
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    tasks = new Tasks();
    killed_ = false;
    num_threads_ = num_threads;
    thread_pool_ = new std::thread[num_threads];
    for (int i = 0; i < num_threads; i++) {
        thread_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spinningThread, this, i);
    }
}

void TaskSystemParallelThreadPoolSpinning::spinningThread(int threadID) {
    int taskID;
    int total;
    while (!killed_)
    {
        // if (killed_) break;
        tasks->mutex_->lock();
        total = tasks->num_total_tasks_;
        taskID = tasks->next_task;
        if (taskID < total) tasks->next_task++;
        tasks->mutex_->unlock();
        if (taskID < total) {
            tasks->runnable_->runTask(taskID, total);
            tasks->mutex_->lock();
            tasks->finished_tasks_++;
            if (tasks->finished_tasks_ == total) {
                done = true;
                tasks->mutex_->unlock();

                // tasks->finishedMutex_->lock();
                // tasks->finishedMutex_->unlock();
                // printf("Notified - Task ID %d - TID : %d\n", taskID, threadID);
                // tasks->finished_->notify_all();

            } else {
                tasks->mutex_->unlock();
            }
        }
    }

    // printf("Threads Exited \n");
    
}



TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // printf("Call to destructor \n");
    killed_ = true;
    for (int i = 0; i < num_threads_; i++)
    thread_pool_[i].join();

    delete[] thread_pool_;
    delete tasks;
    
}



void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    // printf("Call to run \n");
    std::unique_lock<std::mutex> lk(*(tasks->finishedMutex_));
    tasks->mutex_->lock();
    done = false;
    tasks->finished_tasks_ = 0;
    tasks->next_task = 0;
    tasks->num_total_tasks_ = num_total_tasks;
    tasks->runnable_ = runnable;
    tasks->mutex_->unlock();

    // tasks->finished_->wait(lk);
    while(!done);
    // lk.unlock();
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
    tasks = new Tasks();
    killed_ = false;
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
    killed_ = true;
    for (int i = 0; i < num_threads_; i++)
    thread_pool_[i].join();

    delete[] thread_pool_;
    delete tasks;
}

void TaskSystemParallelThreadPoolSleeping::sleepingThread(int threadID) {
    int taskID;
    int total;
    while (!killed_)
    {
        // if (killed_) break;
        tasks->mutex_->lock();
        total = tasks->num_total_tasks_;
        taskID = tasks->next_task;
        if (taskID < total) tasks->next_task++;
        tasks->mutex_->unlock();
        if (taskID < total) {
            tasks->runnable_->runTask(taskID, total);
            tasks->mutex_->lock();
            tasks->finished_tasks_++;
            if (tasks->finished_tasks_ == total) {
                tasks->mutex_->unlock();

                tasks->finishedMutex_->lock();
                tasks->finishedMutex_->unlock();
                // printf("Notified - Task ID %d - TID : %d\n", taskID, threadID);
                tasks->finished_->notify_all();

            } else {
                tasks->mutex_->unlock();
            }
        }
    }

    // printf("Threads Exited \n");
    
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::unique_lock<std::mutex> lk(*(tasks->finishedMutex_));
    tasks->mutex_->lock();
    tasks->finished_tasks_ = 0;
    tasks->next_task = 0;
    tasks->num_total_tasks_ = num_total_tasks;
    tasks->runnable_ = runnable;
    tasks->mutex_->unlock();

    tasks->finished_->wait(lk);
    lk.unlock();
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
