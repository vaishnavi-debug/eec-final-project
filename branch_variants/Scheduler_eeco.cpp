//
//  Scheduler.cpp  —  EECO branch
//  CloudSim
//
//  Energy-Efficient Cloud Orchestration scheduler.
//

#include "Scheduler.hpp"

#include <algorithm>
#include <array>
#include <deque>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using std::array;
using std::deque;
using std::numeric_limits;
using std::remove;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace {

struct VMRecord {
    VMId_t      vm_id;
    MachineId_t machine_id;
    VMType_t    vm_type;
    CPUType_t   cpu_type;
};

vector<MachineId_t>                               g_all_machines;
unordered_map<VMId_t, VMRecord>                   g_vm_records;
unordered_map<MachineId_t, vector<VMId_t>>        g_machine_to_vms;
unordered_map<TaskId_t, VMId_t>                   g_task_to_vm;
unordered_map<TaskId_t, MachineId_t>              g_task_to_machine;
unordered_map<MachineId_t, array<unsigned, NUM_SLAS>> g_machine_sla_counts;
unordered_map<MachineId_t, unsigned>              g_machine_penalty;
unordered_set<MachineId_t>                        g_waking_machines;
unordered_set<MachineId_t>                        g_sleeping_machines;
deque<TaskId_t>                                   g_pending_tasks;
unordered_set<TaskId_t>                           g_pending_set;
int g_active_sla0_count  = 0;
int g_pending_sla0_count = 0;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

size_t CPUIndex(CPUType_t cpu) { return static_cast<size_t>(cpu); }

// Machine is "up" — existing VMs can accept VM_AddTask in either state.
bool IsAttachableState(MachineState_t state) {
    return state == S0 || state == S0i1;
}

// VM_Attach (creating a brand-new VM) requires the machine to be fully active.
// S0i1 is treated as sleep mode by the framework for new-VM attachment.
bool CanAttachNewVM(MachineState_t state) { return state == S0; }

double SafeDiv(double num, double den) {
    return den <= 0.0 ? 0.0 : num / den;
}

double PerfPerWatt(const MachineInfo_t &machine) {
    double perf  = machine.performance.empty() ? 1.0 : machine.performance[P0] * machine.num_cpus;
    double power = 1.0;
    if (!machine.s_states.empty()) power += machine.s_states[S0];
    if (!machine.p_states.empty()) power += machine.num_cpus * machine.p_states[P0];
    return perf / power;
}

Priority_t PriorityForTask(const TaskInfo_t &task) {
    switch (task.required_sla) {
        case SLA0: return HIGH_PRIORITY;
        case SLA1: return MID_PRIORITY;
        case SLA2:
        case SLA3:
        default:   return LOW_PRIORITY;
    }
}

// ---------------------------------------------------------------------------
// EECO capacity limits
// ---------------------------------------------------------------------------

double MaxLoadForTask(const TaskInfo_t &task) {
    switch (task.required_sla) {
        case SLA0: return 0.90;
        case SLA1: return 1.00;
        case SLA2: return 1.25;
        case SLA3: return 1.60;
    }
    return 1.0;
}

double MaxMemoryFractionForTask(const TaskInfo_t &task) {
    switch (task.required_sla) {
        case SLA0: return 0.75;
        case SLA1: return 0.85;
        case SLA2: return 0.90;
        case SLA3: return 0.94;
    }
    return 0.9;
}

// ---------------------------------------------------------------------------
// Machine selection
// ---------------------------------------------------------------------------

bool SupportsTask(const MachineInfo_t &machine, const TaskInfo_t &task) {
    return machine.cpu == task.required_cpu && !(task.gpu_capable && !machine.gpus);
}

VMId_t InvalidVM()      { return numeric_limits<VMId_t>::max(); }
MachineId_t InvalidMachine() { return numeric_limits<MachineId_t>::max(); }

VMId_t FindReusableVM(MachineId_t machine_id, VMType_t vm_type, CPUType_t cpu) {
    auto it = g_machine_to_vms.find(machine_id);
    if (it == g_machine_to_vms.end()) return InvalidVM();
    for (VMId_t vm_id : it->second) {
        auto rec = g_vm_records.find(vm_id);
        if (rec != g_vm_records.end() &&
            rec->second.vm_type == vm_type && rec->second.cpu_type == cpu) {
            return vm_id;
        }
    }
    return InvalidVM();
}

bool HasCapacity(const MachineInfo_t &machine, const TaskInfo_t &task, bool needs_new_vm) {
    const unsigned projected_mem = machine.memory_used + task.required_memory +
                                   (needs_new_vm ? VM_MEMORY_OVERHEAD : 0);
    if (SafeDiv(projected_mem, machine.memory_size) > MaxMemoryFractionForTask(task))
        return false;
    if (SafeDiv(machine.active_tasks + 1, machine.num_cpus) > MaxLoadForTask(task))
        return false;
    return true;
}

// EECO scoring: SLA0/1 prefer loaded machines (consolidation); SLA2/3 prefer
// efficient machines with headroom to minimise energy.
double EecoScore(const MachineInfo_t &machine, const TaskInfo_t &task, bool reuses_vm) {
    const double load   = SafeDiv(machine.active_tasks, machine.num_cpus);
    const double mem    = SafeDiv(machine.memory_used,  machine.memory_size);
    const double eff    = PerfPerWatt(machine);
    const unsigned pen  = g_machine_penalty[machine.machine_id];

    double score;
    if (task.required_sla == SLA0) {
        score = load * 130.0 + mem * 40.0 - eff * 4.0;
    } else if (task.required_sla == SLA1) {
        score = load * 90.0  + mem * 30.0 - eff * 5.0;
    } else {
        score = (1.0 - load) * 80.0 + mem * 25.0 - eff * 6.5;
    }
    if (reuses_vm) score -= 16.0;
    score += 10.0 * pen;
    return score;
}

// Pick the best already-awake machine for the task (lowest score wins).
MachineId_t ChooseScoredAwakeMachine(const TaskInfo_t &task) {
    double best = numeric_limits<double>::max();
    MachineId_t best_id = InvalidMachine();

    for (MachineId_t mid : g_all_machines) {
        if (g_sleeping_machines.count(mid) || g_waking_machines.count(mid)) continue;
        const MachineInfo_t m = Machine_GetInfo(mid);
        if (!IsAttachableState(m.s_state) || !SupportsTask(m, task)) continue;
        const bool reuses = (FindReusableVM(mid, task.required_vm, task.required_cpu) != InvalidVM());
        if (!reuses && !CanAttachNewVM(m.s_state)) continue;
        if (!HasCapacity(m, task, !reuses)) continue;
        const double s = EecoScore(m, task, reuses);
        if (s < best) { best = s; best_id = mid; }
    }
    return best_id;
}

// Pick the best sleeping/idle machine to wake for a non-SLA0 task.
MachineId_t ChooseSleepingMachine(const TaskInfo_t &task) {
    double best = numeric_limits<double>::max();
    MachineId_t best_id = InvalidMachine();

    for (MachineId_t mid : g_all_machines) {
        const MachineInfo_t m = Machine_GetInfo(mid);
        if (g_waking_machines.count(mid) || g_sleeping_machines.count(mid)) continue;
        if (m.s_state == S0 || !SupportsTask(m, task)) continue;
        if (!HasCapacity(m, task, true)) continue;
        // Prefer machines that are efficient and closer to S0 (lower wake cost).
        const double penalty = 20.0 * static_cast<double>(m.s_state)
                             + 18.0 / std::max(0.1, PerfPerWatt(m));
        if (penalty < best) { best = penalty; best_id = mid; }
    }
    return best_id;
}

// ---------------------------------------------------------------------------
// Task assignment
// ---------------------------------------------------------------------------

void SetMachinePerformance(MachineId_t machine_id, CPUPerformance_t state) {
    const MachineInfo_t info = Machine_GetInfo(machine_id);
    if (info.p_state == state) return;
    for (unsigned core = 0; core < info.num_cpus; ++core)
        Machine_SetCorePerformance(machine_id, core, state);
}

void TrackAssignment(MachineId_t machine_id, VMId_t vm_id, const TaskInfo_t &task) {
    g_task_to_vm[task.task_id]      = vm_id;
    g_task_to_machine[task.task_id] = machine_id;
    g_machine_sla_counts[machine_id][task.required_sla]++;
    if (task.required_sla == SLA0) g_active_sla0_count++;
}

bool AssignToMachine(MachineId_t machine_id, TaskId_t task_id) {
    const TaskInfo_t task = GetTaskInfo(task_id);
    VMId_t vm_id = FindReusableVM(machine_id, task.required_vm, task.required_cpu);

    if (vm_id == InvalidVM()) {
        const MachineInfo_t recheck = Machine_GetInfo(machine_id);
        if (!CanAttachNewVM(recheck.s_state) ||
            g_sleeping_machines.count(machine_id) || g_waking_machines.count(machine_id))
            return false;
        vm_id = VM_Create(task.required_vm, task.required_cpu);
        VM_Attach(vm_id, machine_id);
        g_vm_records[vm_id] = VMRecord{vm_id, machine_id, task.required_vm, task.required_cpu};
        g_machine_to_vms[machine_id].push_back(vm_id);
    }

    VM_AddTask(vm_id, task_id, PriorityForTask(task));
    TrackAssignment(machine_id, vm_id, task);
    return true;
}

// high_priority pushes SLA0 tasks to the front so they are dispatched first.
void QueueTask(TaskId_t task_id, bool high_priority = false) {
    if (g_pending_set.insert(task_id).second) {
        if (high_priority) {
            g_pending_tasks.push_front(task_id);
            g_pending_sla0_count++;
        } else {
            g_pending_tasks.push_back(task_id);
        }
    }
}

bool TryAssignTask(TaskId_t task_id, bool allow_wake) {
    const TaskInfo_t task = GetTaskInfo(task_id);
    MachineId_t mid = ChooseScoredAwakeMachine(task);
    if (mid != InvalidMachine()) return AssignToMachine(mid, task_id);

    if (allow_wake) {
        if (task.required_sla == SLA0) {
            // Wake every compatible machine at once so all cores become available
            // as quickly as possible during a high-priority burst.
            for (MachineId_t m : g_all_machines) {
                if (g_waking_machines.count(m)) continue;
                if (!g_sleeping_machines.count(m)) {
                    if (Machine_GetInfo(m).s_state == S0) continue;
                }
                if (!SupportsTask(Machine_GetInfo(m), task)) continue;
                g_waking_machines.insert(m);
                g_sleeping_machines.erase(m);
                Machine_SetState(m, S0);
            }
        } else {
            mid = ChooseSleepingMachine(task);
            if (mid != InvalidMachine()) {
                g_waking_machines.insert(mid);
                Machine_SetState(mid, S0);
            }
        }
    }
    return false;
}

void DispatchPendingTasks() {
    if (g_pending_tasks.empty()) return;

    // Process exactly the snapshot of tasks that were pending when we entered
    // so that re-queued tasks don't cause unbounded iteration.
    const size_t pending = g_pending_tasks.size();
    for (size_t i = 0; i < pending; ++i) {
        const TaskId_t task_id = g_pending_tasks.front();
        g_pending_tasks.pop_front();
        g_pending_set.erase(task_id);

        if (IsTaskCompleted(task_id)) {
            if (g_pending_sla0_count > 0) {
                if (GetTaskInfo(task_id).required_sla == SLA0) g_pending_sla0_count--;
            }
            continue;
        }

        const TaskInfo_t t  = GetTaskInfo(task_id);
        const bool is_sla0  = (t.required_sla == SLA0);
        if (is_sla0 && g_pending_sla0_count > 0) g_pending_sla0_count--;

        if (!TryAssignTask(task_id, true)) QueueTask(task_id, is_sla0);
    }
}

// ---------------------------------------------------------------------------
// VM lifecycle
// ---------------------------------------------------------------------------

void DropEmptyVM(VMId_t vm_id) {
    auto rec_it = g_vm_records.find(vm_id);
    if (rec_it == g_vm_records.end()) return;

    const MachineId_t machine_id = rec_it->second.machine_id;
    // VM_Shutdown calls DetachVM which crashes unless the machine is in S0.
    // Leave the VM as a hot-spare in other states; ManageIdleMachines will
    // clean it up before putting the machine to sleep.
    if (Machine_GetInfo(machine_id).s_state != S0) return;

    VM_Shutdown(vm_id);
    g_vm_records.erase(rec_it);
    auto map_it = g_machine_to_vms.find(machine_id);
    if (map_it != g_machine_to_vms.end()) {
        auto &vms = map_it->second;
        vms.erase(remove(vms.begin(), vms.end(), vm_id), vms.end());
    }
}

// ---------------------------------------------------------------------------
// Power management
// ---------------------------------------------------------------------------

void TuneMachinePower(MachineId_t machine_id) {
    const MachineInfo_t machine = Machine_GetInfo(machine_id);
    if (machine.active_tasks == 0) return;

    const double load = SafeDiv(machine.active_tasks, machine.num_cpus);
    const auto sla_it = g_machine_sla_counts.find(machine_id);
    const array<unsigned, NUM_SLAS> counts =
        (sla_it == g_machine_sla_counts.end())
        ? array<unsigned, NUM_SLAS>{0, 0, 0, 0}
        : sla_it->second;

    CPUPerformance_t target;
    if (counts[SLA0] > 0 || load > 0.85)     target = P0;
    else if (counts[SLA1] > 0 || load > 0.60) target = P1;
    else if (load > 0.35)                      target = P2;
    else                                       target = P3;

    SetMachinePerformance(machine_id, target);
}

void ManageIdleMachines() {
    // Block all sleep transitions while SLA0 tasks are active or pending
    // to prevent wakeup latency from causing priority violations.
    if (g_active_sla0_count > 0 || g_pending_sla0_count > 0) return;

    // Count idle attachable machines per CPU type.
    array<unsigned, 4> idle_by_cpu = {0, 0, 0, 0};
    for (MachineId_t mid : g_all_machines) {
        const MachineInfo_t m = Machine_GetInfo(mid);
        if (IsAttachableState(m.s_state) && m.active_tasks == 0 && m.active_vms == 0)
            idle_by_cpu[CPUIndex(m.cpu)]++;
    }

    // Keep exactly 1 warm idle machine per CPU type in S0 for immediate VM
    // attachment (CanAttachNewVM(S0i1) == false, so S0i1 still forces a full
    // wakeup round-trip). Sleep the rest to S3 to save energy.
    for (MachineId_t machine_id : g_all_machines) {
        const MachineInfo_t machine = Machine_GetInfo(machine_id);
        if (machine.active_tasks != 0 || machine.active_vms != 0 ||
            g_waking_machines.count(machine_id) || g_sleeping_machines.count(machine_id))
            continue;
        if (!IsAttachableState(machine.s_state)) continue;

        // Skip if a pending task completion is still in flight for this machine
        // (framework zeroes active_tasks before firing TaskComplete).
        const auto sla_it = g_machine_sla_counts.find(machine_id);
        if (sla_it != g_machine_sla_counts.end()) {
            const auto &c = sla_it->second;
            if (c[0] || c[1] || c[2] || c[3]) continue;
        }

        // Skip if hot-spare VMs still tracked on this machine.
        const auto vm_it = g_machine_to_vms.find(machine_id);
        const bool has_vms = (vm_it != g_machine_to_vms.end() && !vm_it->second.empty());

        const size_t cpu_idx = CPUIndex(machine.cpu);
        if (idle_by_cpu[cpu_idx] > 1) {
            if (!has_vms) {
                g_sleeping_machines.insert(machine_id);
                Machine_SetState(machine_id, S3);
                idle_by_cpu[cpu_idx]--;
            }
        }
        // The one warm machine per CPU type stays in S0 — no transition needed.
    }
}

// ---------------------------------------------------------------------------
// State reset
// ---------------------------------------------------------------------------

void ResetState() {
    g_all_machines.clear();
    g_vm_records.clear();
    g_machine_to_vms.clear();
    g_task_to_vm.clear();
    g_task_to_machine.clear();
    g_machine_sla_counts.clear();
    g_machine_penalty.clear();
    g_waking_machines.clear();
    g_sleeping_machines.clear();
    g_pending_tasks.clear();
    g_pending_set.clear();
    g_active_sla0_count  = 0;
    g_pending_sla0_count = 0;
}

}  // namespace

// ---------------------------------------------------------------------------
// Scheduler interface
// ---------------------------------------------------------------------------

void Scheduler::Init() {
    ResetState();
    machines.clear();
    vms.clear();
    SimOutput("Scheduler::Init(): using algorithm eeco", 1);

    const unsigned total = Machine_GetTotal();
    for (unsigned i = 0; i < total; ++i) {
        const MachineId_t machine_id = MachineId_t(i);
        const MachineInfo_t machine  = Machine_GetInfo(machine_id);
        machines.push_back(machine_id);
        g_all_machines.push_back(machine_id);
        g_machine_sla_counts[machine_id] = {0, 0, 0, 0};

        // Start all machines beyond the first 4 in deep sleep to save energy.
        if (i >= 4 && machine.s_state == S0) {
            g_sleeping_machines.insert(machine_id);
            Machine_SetState(machine_id, S3);
        }
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    (void)time; (void)vm_id;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    (void)now;
    if (!TryAssignTask(task_id, true)) {
        const bool high_pri = (GetTaskInfo(task_id).required_sla == SLA0);
        QueueTask(task_id, high_pri);
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    (void)now;
    DispatchPendingTasks();
    for (MachineId_t mid : g_all_machines) TuneMachinePower(mid);
    ManageIdleMachines();
}

void Scheduler::Shutdown(Time_t time) {
    (void)time;
    for (const auto &entry : g_vm_records) {
        if (Machine_GetInfo(entry.second.machine_id).s_state == S0)
            VM_Shutdown(entry.first);
    }
    SimOutput("SimulationComplete(): algorithm=eeco", 1);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    (void)now;
    auto vm_it      = g_task_to_vm.find(task_id);
    auto machine_it = g_task_to_machine.find(task_id);
    if (vm_it == g_task_to_vm.end() || machine_it == g_task_to_machine.end()) return;

    const VMId_t      vm_id      = vm_it->second;
    const MachineId_t machine_id = machine_it->second;
    const TaskInfo_t  task       = GetTaskInfo(task_id);

    // Guard before removing — the framework may have already cleaned up the task.
    const VMInfo_t vm_before = VM_GetInfo(vm_id);
    for (const TaskId_t t : vm_before.active_tasks) {
        if (t == task_id) { VM_RemoveTask(vm_id, task_id); break; }
    }

    auto sla_it = g_machine_sla_counts.find(machine_id);
    if (sla_it != g_machine_sla_counts.end() && sla_it->second[task.required_sla] > 0)
        sla_it->second[task.required_sla]--;
    if (task.required_sla == SLA0 && g_active_sla0_count > 0)
        g_active_sla0_count--;

    g_task_to_vm.erase(vm_it);
    g_task_to_machine.erase(machine_it);

    if (VM_GetInfo(vm_id).active_tasks.empty()) DropEmptyVM(vm_id);

    // Fill the freed slot immediately rather than waiting for the next
    // PeriodicCheck tick.
    if (!g_pending_tasks.empty()) DispatchPendingTasks();
}

// ---------------------------------------------------------------------------
// Framework callbacks
// ---------------------------------------------------------------------------

static Scheduler SchedulerInstance;

void InitScheduler()                                    { SchedulerInstance.Init(); }
void HandleNewTask(Time_t t, TaskId_t id)               { SchedulerInstance.NewTask(t, id); }
void HandleTaskCompletion(Time_t t, TaskId_t id)        { SchedulerInstance.TaskComplete(t, id); }
void MigrationDone(Time_t t, VMId_t id)                 { SchedulerInstance.MigrationComplete(t, id); }
void SchedulerCheck(Time_t t)                           { SchedulerInstance.PeriodicCheck(t); }

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    (void)time;
    g_machine_penalty[machine_id] += 3;
    SetMachinePerformance(machine_id, P0);
}

void SimulationComplete(Time_t time) {
    cout << "Algorithm: eeco" << endl;
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time) / 1000000 << " seconds" << endl;
    SchedulerInstance.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    (void)time;
    const auto it = g_task_to_machine.find(task_id);
    if (it != g_task_to_machine.end()) {
        g_machine_penalty[it->second] += 2;
        SetMachinePerformance(it->second, P0);
    }
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    (void)time;
    // Only dispatch when a machine finishes waking up (S3 → S0 or S0i1 → S0).
    // Sleep transitions also fire this callback but no new capacity becomes
    // available there, so dispatching is wasteful.
    const bool was_waking = g_waking_machines.count(machine_id) > 0;
    g_waking_machines.erase(machine_id);
    g_sleeping_machines.erase(machine_id);
    if (was_waking) DispatchPendingTasks();
}
