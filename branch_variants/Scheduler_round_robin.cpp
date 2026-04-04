//
//  Scheduler.cpp
//  CloudSim
//
//  Multi-algorithm scheduler scaffold.
//  Put this file on four different branches and only change kSelectedAlgorithm.
//

#include "Scheduler.hpp"

#include <algorithm>
#include <array>
#include <deque>
#include <limits>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using std::array;
using std::deque;
using std::map;
using std::numeric_limits;
using std::remove;
using std::string;
using std::to_string;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace {

enum class Algorithm {
    ROUND_ROBIN,
    GREEDY,
    EECO,
    PMAPPER
};

// Change this line per branch:
// round-robin branch -> Algorithm::ROUND_ROBIN
// greedy branch      -> Algorithm::GREEDY
// eeco branch        -> Algorithm::EECO
// pmapper branch     -> Algorithm::PMAPPER
static constexpr Algorithm kSelectedAlgorithm = Algorithm::ROUND_ROBIN;

struct VMRecord {
    VMId_t vm_id;
    MachineId_t machine_id;
    VMType_t vm_type;
    CPUType_t cpu_type;
};

vector<MachineId_t> g_all_machines;
unordered_map<VMId_t, VMRecord> g_vm_records;
unordered_map<MachineId_t, vector<VMId_t>> g_machine_to_vms;
unordered_map<TaskId_t, VMId_t> g_task_to_vm;
unordered_map<TaskId_t, MachineId_t> g_task_to_machine;
unordered_map<MachineId_t, array<unsigned, NUM_SLAS>> g_machine_sla_counts;
unordered_map<MachineId_t, unsigned> g_machine_penalty;
unordered_set<MachineId_t> g_waking_machines;
unordered_set<MachineId_t> g_sleeping_machines;
deque<TaskId_t> g_pending_tasks;
unordered_set<TaskId_t> g_pending_set;
array<unsigned, 4> g_rr_cursor = {0, 0, 0, 0};

const char *AlgorithmName() {
    switch (kSelectedAlgorithm) {
        case Algorithm::ROUND_ROBIN: return "round_robin";
        case Algorithm::GREEDY: return "greedy";
        case Algorithm::EECO: return "eeco";
        case Algorithm::PMAPPER: return "pmapper";
    }
    return "unknown";
}

size_t CPUIndex(CPUType_t cpu) {
    return static_cast<size_t>(cpu);
}

// Machine is "up" – existing VMs can accept VM_AddTask in either state.
bool IsAttachableState(MachineState_t state) {
    return state == S0 || state == S0i1;
}

// VM_Attach (creating a brand-new VM) requires the machine to be fully active (S0 only).
// S0i1 is treated as sleep mode by the framework for attachment purposes.
bool CanAttachNewVM(MachineState_t state) {
    return state == S0;
}

double SafeDiv(double num, double den) {
    return den <= 0.0 ? 0.0 : num / den;
}

double PerfPerWatt(const MachineInfo_t &machine) {
    double perf = machine.performance.empty() ? 1.0 : machine.performance[P0] * machine.num_cpus;
    double power = 1.0;
    if (!machine.s_states.empty()) {
        power += machine.s_states[S0];
    }
    if (!machine.p_states.empty()) {
        power += machine.num_cpus * machine.p_states[P0];
    }
    return perf / power;
}

Priority_t PriorityForTask(const TaskInfo_t &task) {
    switch (task.required_sla) {
        case SLA0: return HIGH_PRIORITY;
        case SLA1: return MID_PRIORITY;
        case SLA2:
        case SLA3:
        default: return LOW_PRIORITY;
    }
}

double MaxLoadForTask(const TaskInfo_t &task) {
    switch (kSelectedAlgorithm) {
        case Algorithm::ROUND_ROBIN:
            switch (task.required_sla) {
                case SLA0: return 0.95;
                case SLA1: return 1.15;
                case SLA2: return 1.40;
                case SLA3: return 1.80;
            }
            break;
        case Algorithm::GREEDY:
            switch (task.required_sla) {
                case SLA0: return 0.85;
                case SLA1: return 1.00;
                case SLA2: return 1.30;
                case SLA3: return 1.70;
            }
            break;
        case Algorithm::EECO:
            switch (task.required_sla) {
                case SLA0: return 0.90;
                case SLA1: return 1.00;
                case SLA2: return 1.25;
                case SLA3: return 1.60;
            }
            break;
        case Algorithm::PMAPPER:
            switch (task.required_sla) {
                case SLA0: return 0.80;
                case SLA1: return 1.00;
                case SLA2: return 1.25;
                case SLA3: return 1.55;
            }
            break;
    }
    return 1.0;
}

double MaxMemoryFractionForTask(const TaskInfo_t &task) {
    switch (kSelectedAlgorithm) {
        case Algorithm::ROUND_ROBIN:
            return 0.96;
        case Algorithm::GREEDY:
            switch (task.required_sla) {
                case SLA0: return 0.85;
                case SLA1: return 0.90;
                case SLA2:
                case SLA3:
                default: return 0.94;
            }
        case Algorithm::EECO:
        case Algorithm::PMAPPER:
            switch (task.required_sla) {
                case SLA0: return 0.75;
                case SLA1: return 0.85;
                case SLA2: return 0.90;
                case SLA3: return 0.94;
            }
    }
    return 0.9;
}

bool SupportsTask(const MachineInfo_t &machine, const TaskInfo_t &task) {
    if (machine.cpu != task.required_cpu) {
        return false;
    }
    if (task.gpu_capable && !machine.gpus) {
        return false;
    }
    return true;
}

VMId_t InvalidVM() {
    return numeric_limits<VMId_t>::max();
}

MachineId_t InvalidMachine() {
    return numeric_limits<MachineId_t>::max();
}

VMId_t FindReusableVM(MachineId_t machine_id, VMType_t vm_type, CPUType_t cpu) {
    auto it = g_machine_to_vms.find(machine_id);
    if (it == g_machine_to_vms.end()) {
        return InvalidVM();
    }

    for (VMId_t vm_id : it->second) {
        const auto record_it = g_vm_records.find(vm_id);
        if (record_it == g_vm_records.end()) {
            continue;
        }
        const VMRecord &record = record_it->second;
        if (record.vm_type == vm_type && record.cpu_type == cpu) {
            return vm_id;
        }
    }

    return InvalidVM();
}

bool HasCapacity(const MachineInfo_t &machine, const TaskInfo_t &task, bool needs_new_vm) {
    const unsigned projected_memory = machine.memory_used + task.required_memory + (needs_new_vm ? VM_MEMORY_OVERHEAD : 0);
    const double projected_memory_fraction = SafeDiv(static_cast<double>(projected_memory), static_cast<double>(machine.memory_size));
    if (projected_memory_fraction > MaxMemoryFractionForTask(task)) {
        return false;
    }

    const double projected_load = SafeDiv(static_cast<double>(machine.active_tasks + 1), static_cast<double>(machine.num_cpus));
    if (projected_load > MaxLoadForTask(task)) {
        return false;
    }

    return true;
}

double GreedyScore(const MachineInfo_t &machine, bool reuses_vm) {
    const double load_fraction = SafeDiv(static_cast<double>(machine.active_tasks), static_cast<double>(machine.num_cpus));
    const double memory_fraction = SafeDiv(static_cast<double>(machine.memory_used), static_cast<double>(machine.memory_size));
    double score = (1.0 - load_fraction) * 100.0 + memory_fraction * 30.0;
    if (reuses_vm) {
        score -= 20.0;
    }
    score += 6.0 * g_machine_penalty[machine.machine_id];
    return score;
}

double EecoScore(const MachineInfo_t &machine, const TaskInfo_t &task, bool reuses_vm) {
    const double load_fraction = SafeDiv(static_cast<double>(machine.active_tasks), static_cast<double>(machine.num_cpus));
    const double memory_fraction = SafeDiv(static_cast<double>(machine.memory_used), static_cast<double>(machine.memory_size));
    const double efficiency = PerfPerWatt(machine);
    const unsigned penalty = g_machine_penalty[machine.machine_id];

    double score = 0.0;
    if (task.required_sla == SLA0) {
        score = load_fraction * 130.0 + memory_fraction * 40.0 - efficiency * 4.0;
    } else if (task.required_sla == SLA1) {
        score = load_fraction * 90.0 + memory_fraction * 30.0 - efficiency * 5.0;
    } else {
        score = (1.0 - load_fraction) * 80.0 + memory_fraction * 25.0 - efficiency * 6.5;
    }

    if (reuses_vm) {
        score -= 16.0;
    }
    score += 10.0 * penalty;
    return score;
}

// PMapper: power-aware placement using a linear power model with strong consolidation.
// Assigns tasks to servers that minimize power increase while maximising utilization,
// reducing the number of active servers (Best Fit Decreasing on power).
double PMapperScore(const MachineInfo_t &machine, const TaskInfo_t &task, bool reuses_vm) {
    const double p_idle = machine.s_states.empty() ? 0.0 : machine.s_states[S0];
    const double p_cpu  = machine.p_states.empty() ? 0.0 : machine.p_states[P0];

    // Linear power model: P(u) = P_idle + P_dyn_max * u
    const double p_dyn_max = p_cpu * machine.num_cpus;
    const double u_current = SafeDiv(static_cast<double>(machine.active_tasks),     static_cast<double>(machine.num_cpus));
    const double u_next    = SafeDiv(static_cast<double>(machine.active_tasks + 1), static_cast<double>(machine.num_cpus));
    const double delta_power = p_dyn_max * (u_next - u_current);

    // Consolidation: strongly prefer already-loaded machines to minimise active server count.
    const double remaining_capacity = 1.0 - u_current;
    const double consolidation = remaining_capacity * 45.0;

    const double memory_fraction = SafeDiv(static_cast<double>(machine.memory_used), static_cast<double>(machine.memory_size));

    // SLA-aware bias: high-SLA tasks prefer responsive (less loaded) machines slightly.
    const double sla_bias = (task.required_sla == SLA0) ? -8.0 : (task.required_sla == SLA1 ? -3.0 : 0.0);

    // Perf-per-watt bonus: favour energy-efficient machines.
    const double efficiency = (p_idle + p_dyn_max > 0.0) ? (p_dyn_max / (p_idle + p_dyn_max)) : 0.0;

    double score = delta_power + consolidation + memory_fraction * 18.0 + sla_bias - efficiency * 3.0;
    score += 12.0 * g_machine_penalty[machine.machine_id];
    if (reuses_vm) {
        score -= 10.0;
    }
    return score;
}

double ScoreMachine(const MachineInfo_t &machine, const TaskInfo_t &task, bool reuses_vm) {
    switch (kSelectedAlgorithm) {
        case Algorithm::GREEDY:
            return GreedyScore(machine, reuses_vm);
        case Algorithm::EECO:
            return EecoScore(machine, task, reuses_vm);
        case Algorithm::PMAPPER:
            return PMapperScore(machine, task, reuses_vm);
        case Algorithm::ROUND_ROBIN:
        default:
            return 0.0;
    }
}

MachineId_t ChooseRoundRobinMachine(const TaskInfo_t &task) {
    const size_t cpu_index = CPUIndex(task.required_cpu);
    if (g_all_machines.empty()) {
        return InvalidMachine();
    }

    for (size_t offset = 0; offset < g_all_machines.size(); ++offset) {
        const size_t idx = (g_rr_cursor[cpu_index] + offset) % g_all_machines.size();
        const MachineId_t machine_id = g_all_machines[idx];
        if (g_sleeping_machines.count(machine_id) || g_waking_machines.count(machine_id)) {
            continue;
        }
        const MachineInfo_t machine = Machine_GetInfo(machine_id);
        if (!IsAttachableState(machine.s_state)) {
            continue;
        }
        if (!SupportsTask(machine, task)) {
            continue;
        }

        const bool needs_new_vm = (FindReusableVM(machine_id, task.required_vm, task.required_cpu) == InvalidVM());
        if (needs_new_vm && !CanAttachNewVM(machine.s_state)) {
            continue;
        }
        if (!HasCapacity(machine, task, needs_new_vm)) {
            continue;
        }

        g_rr_cursor[cpu_index] = static_cast<unsigned>((idx + 1) % g_all_machines.size());
        return machine_id;
    }

    return InvalidMachine();
}

MachineId_t ChooseScoredAwakeMachine(const TaskInfo_t &task) {
    double best_score = numeric_limits<double>::max();
    MachineId_t best_machine = InvalidMachine();

    for (MachineId_t machine_id : g_all_machines) {
        if (g_sleeping_machines.count(machine_id) || g_waking_machines.count(machine_id)) {
            continue;
        }
        const MachineInfo_t machine = Machine_GetInfo(machine_id);
        if (!IsAttachableState(machine.s_state)) {
            continue;
        }
        if (!SupportsTask(machine, task)) {
            continue;
        }

        const bool reuses_vm = (FindReusableVM(machine_id, task.required_vm, task.required_cpu) != InvalidVM());
        if (!reuses_vm && !CanAttachNewVM(machine.s_state)) {
            continue;
        }
        if (!HasCapacity(machine, task, !reuses_vm)) {
            continue;
        }

        const double score = ScoreMachine(machine, task, reuses_vm);
        if (score < best_score) {
            best_score = score;
            best_machine = machine_id;
        }
    }

    return best_machine;
}

MachineId_t ChooseSleepingMachine(const TaskInfo_t &task) {
    double best_score = numeric_limits<double>::max();
    MachineId_t best_machine = InvalidMachine();

    for (MachineId_t machine_id : g_all_machines) {
        const MachineInfo_t machine = Machine_GetInfo(machine_id);
        if (g_waking_machines.count(machine_id) != 0 || g_sleeping_machines.count(machine_id) != 0) {
            continue;
        }
        // Skip S0 machines (already fully active and handled by ChooseScoredAwakeMachine).
        // Include S0i1 machines — they need waking to S0 before a new VM can be attached.
        if (machine.s_state == S0) {
            continue;
        }
        if (!SupportsTask(machine, task)) {
            continue;
        }
        if (!HasCapacity(machine, task, true)) {
            continue;
        }

        double wake_penalty = 20.0 * static_cast<double>(machine.s_state);
        if (kSelectedAlgorithm == Algorithm::ROUND_ROBIN) {
            wake_penalty += static_cast<double>(machine.machine_id);
        } else if (kSelectedAlgorithm == Algorithm::GREEDY) {
            wake_penalty += 25.0 / std::max(0.1, PerfPerWatt(machine));
        } else {
            wake_penalty += 18.0 / std::max(0.1, PerfPerWatt(machine));
        }

        if (wake_penalty < best_score) {
            best_score = wake_penalty;
            best_machine = machine_id;
        }
    }

    return best_machine;
}

MachineId_t ChooseMachineForTask(const TaskInfo_t &task) {
    if (kSelectedAlgorithm == Algorithm::ROUND_ROBIN) {
        return ChooseRoundRobinMachine(task);
    }
    return ChooseScoredAwakeMachine(task);
}

void SetMachinePerformance(MachineId_t machine_id, CPUPerformance_t state) {
    const MachineInfo_t info = Machine_GetInfo(machine_id);
    if (info.p_state == state) {
        return;
    }
    for (unsigned core = 0; core < info.num_cpus; ++core) {
        Machine_SetCorePerformance(machine_id, core, state);
    }
}

void TrackAssignment(MachineId_t machine_id, VMId_t vm_id, const TaskInfo_t &task) {
    g_task_to_vm[task.task_id] = vm_id;
    g_task_to_machine[task.task_id] = machine_id;
    g_machine_sla_counts[machine_id][task.required_sla]++;
}

bool AssignToMachine(MachineId_t machine_id, TaskId_t task_id) {
    const TaskInfo_t task = GetTaskInfo(task_id);
    VMId_t vm_id = FindReusableVM(machine_id, task.required_vm, task.required_cpu);

    if (vm_id == InvalidVM()) {
        const MachineInfo_t recheck = Machine_GetInfo(machine_id);
        if (!CanAttachNewVM(recheck.s_state) ||
            g_sleeping_machines.count(machine_id) || g_waking_machines.count(machine_id)) {
            return false;
        }
        vm_id = VM_Create(task.required_vm, task.required_cpu);
        VM_Attach(vm_id, machine_id);
        g_vm_records[vm_id] = VMRecord{vm_id, machine_id, task.required_vm, task.required_cpu};
        g_machine_to_vms[machine_id].push_back(vm_id);
    }

    VM_AddTask(vm_id, task_id, PriorityForTask(task));
    TrackAssignment(machine_id, vm_id, task);
    return true;
}

void QueueTask(TaskId_t task_id) {
    if (g_pending_set.insert(task_id).second) {
        g_pending_tasks.push_back(task_id);
    }
}

bool TryAssignTask(TaskId_t task_id, bool allow_wake) {
    const TaskInfo_t task = GetTaskInfo(task_id);
    MachineId_t machine_id = ChooseMachineForTask(task);
    if (machine_id != InvalidMachine()) {
        return AssignToMachine(machine_id, task_id);
    }

    if (allow_wake) {
        if (task.required_sla == SLA0) {
            // For SLA0 tasks wake EVERY suitable sleeping machine at once so
            // all cores become available as soon as possible.
            for (MachineId_t mid : g_all_machines) {
                if (g_waking_machines.count(mid)) continue;
                if (!g_sleeping_machines.count(mid)) {
                    const MachineInfo_t m = Machine_GetInfo(mid);
                    if (m.s_state == S0 || m.s_state == S0i1) continue;
                }
                const MachineInfo_t m = Machine_GetInfo(mid);
                if (!SupportsTask(m, task)) continue;
                g_waking_machines.insert(mid);
                g_sleeping_machines.erase(mid);
                Machine_SetState(mid, S0);
            }
        } else {
            machine_id = ChooseSleepingMachine(task);
            if (machine_id != InvalidMachine()) {
                g_waking_machines.insert(machine_id);
                Machine_SetState(machine_id, S0);
            }
        }
    }

    return false;
}

void DispatchPendingTasks() {
    if (g_pending_tasks.empty()) {
        return;
    }

    // Sort by SLA urgency (SLA0 first) so the highest-priority tasks get
    // machines before lower-priority ones when capacity is limited.
    sort(g_pending_tasks.begin(), g_pending_tasks.end(),
         [](TaskId_t a, TaskId_t b) {
             return GetTaskInfo(a).required_sla < GetTaskInfo(b).required_sla;
         });

    const size_t pending = g_pending_tasks.size();
    for (size_t i = 0; i < pending; ++i) {
        const TaskId_t task_id = g_pending_tasks.front();
        g_pending_tasks.pop_front();
        g_pending_set.erase(task_id);

        if (IsTaskCompleted(task_id)) {
            continue;
        }
        // Allow SLA0 pending tasks to wake sleeping machines — the standard
        // path (NewTask) only wakes one machine per arrival, which is too slow
        // for sudden high-priority bursts.
        const bool allow_wake = (GetTaskInfo(task_id).required_sla == SLA0);
        if (!TryAssignTask(task_id, allow_wake)) {
            QueueTask(task_id);
        }
    }
}

void DropEmptyVM(VMId_t vm_id) {
    const auto record_it = g_vm_records.find(vm_id);
    if (record_it == g_vm_records.end()) {
        return;
    }

    const MachineId_t machine_id = record_it->second.machine_id;
    const MachineInfo_t machine = Machine_GetInfo(machine_id);

    // VM_Shutdown internally calls DetachVM, which the framework rejects unless the
    // machine is fully active (S0). In any other state leave the VM in place as a
    // hot-spare so it can be reused; ManageIdleMachines will clean it up before the
    // machine sleeps to S3.
    if (machine.s_state != S0) {
        return;
    }

    VM_Shutdown(vm_id);
    g_vm_records.erase(record_it);

    auto map_it = g_machine_to_vms.find(machine_id);
    if (map_it != g_machine_to_vms.end()) {
        vector<VMId_t> &vm_list = map_it->second;
        vm_list.erase(remove(vm_list.begin(), vm_list.end(), vm_id), vm_list.end());
    }
}

void TuneMachinePower(MachineId_t machine_id) {
    const MachineInfo_t machine = Machine_GetInfo(machine_id);
    if (machine.active_tasks == 0) {
        return;
    }

    const double load_fraction = SafeDiv(static_cast<double>(machine.active_tasks), static_cast<double>(machine.num_cpus));
    const auto sla_it = g_machine_sla_counts.find(machine_id);
    const array<unsigned, NUM_SLAS> counts = (sla_it == g_machine_sla_counts.end()) ? array<unsigned, NUM_SLAS>{0, 0, 0, 0} : sla_it->second;

    CPUPerformance_t target = P0;
    if (kSelectedAlgorithm == Algorithm::ROUND_ROBIN) {
        target = P0;
    } else if (counts[SLA0] > 0 || load_fraction > 0.85) {
        target = P0;
    } else if (counts[SLA1] > 0 || load_fraction > 0.60) {
        target = P1;
    } else if (load_fraction > 0.35) {
        target = (kSelectedAlgorithm == Algorithm::GREEDY) ? P1 : P2;
    } else {
        target = (kSelectedAlgorithm == Algorithm::EECO || kSelectedAlgorithm == Algorithm::PMAPPER) ? P3 : P2;
    }

    SetMachinePerformance(machine_id, target);
}

unsigned WarmIdleTarget() {
    switch (kSelectedAlgorithm) {
        case Algorithm::ROUND_ROBIN: return 1000;
        case Algorithm::GREEDY: return 0;
        case Algorithm::EECO: return 1;
        case Algorithm::PMAPPER: return 1;
    }
    return 1;
}

void ManageIdleMachines() {
    if (kSelectedAlgorithm == Algorithm::ROUND_ROBIN) {
        return;
    }

    // Don't sleep or idle-step any machine while SLA0 tasks are in the system
    // (either pending or currently running). Sleeping machines during a high-
    // priority burst causes wakeup latency that directly drives SLA violations.
    for (TaskId_t tid : g_pending_tasks) {
        if (!IsTaskCompleted(tid) && GetTaskInfo(tid).required_sla == SLA0) {
            return;
        }
    }
    for (const auto &entry : g_machine_sla_counts) {
        if (entry.second[SLA0] > 0) {
            return;
        }
    }

    array<unsigned, 4> idle_attachable_by_cpu = {0, 0, 0, 0};
    for (MachineId_t machine_id : g_all_machines) {
        const MachineInfo_t machine = Machine_GetInfo(machine_id);
        if (IsAttachableState(machine.s_state) && machine.active_tasks == 0 && machine.active_vms == 0) {
            idle_attachable_by_cpu[CPUIndex(machine.cpu)]++;
        }
    }

    const unsigned warm_target = WarmIdleTarget();
    for (MachineId_t machine_id : g_all_machines) {
        const MachineInfo_t machine = Machine_GetInfo(machine_id);
        const size_t cpu_index = CPUIndex(machine.cpu);

        if (machine.active_tasks != 0 || machine.active_vms != 0 ||
            g_waking_machines.count(machine_id) != 0 || g_sleeping_machines.count(machine_id) != 0) {
            continue;
        }
        if (!IsAttachableState(machine.s_state)) {
            continue;
        }

        // The framework zeros active_tasks/active_vms when a task's time expires,
        // *before* our TaskComplete callback fires.  g_machine_sla_counts is only
        // decremented inside TaskComplete, so a non-zero entry means at least one
        // task completion is still pending — skip the machine entirely to avoid
        // racing with that callback (DetachVM / AttachVM both crash in sleep mode).
        const auto sla_it = g_machine_sla_counts.find(machine_id);
        if (sla_it != g_machine_sla_counts.end()) {
            const auto &c = sla_it->second;
            if (c[0] != 0 || c[1] != 0 || c[2] != 0 || c[3] != 0) {
                continue;
            }
        }

        // Also skip if our own VM tracking still has VMs on this machine (hot-spare
        // VMs that DropEmptyVM left because the machine was in S0i1 at the time).
        const auto vm_track_it = g_machine_to_vms.find(machine_id);
        const bool has_tracked_vms = (vm_track_it != g_machine_to_vms.end() &&
                                      !vm_track_it->second.empty());

        if (idle_attachable_by_cpu[cpu_index] > warm_target) {
            if (!has_tracked_vms) {
                g_sleeping_machines.insert(machine_id);
                Machine_SetState(machine_id, S3);
                idle_attachable_by_cpu[cpu_index]--;
            }
        } else if (machine.s_state != S0i1) {
            Machine_SetState(machine_id, S0i1);
        }
    }
}

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
    g_rr_cursor = {0, 0, 0, 0};
}

}  // namespace

void Scheduler::Init() {
    ResetState();
    machines.clear();
    vms.clear();
    SimOutput("Scheduler::Init(): using algorithm " + string(AlgorithmName()), 1);

    const unsigned total = Machine_GetTotal();
    for (unsigned i = 0; i < total; ++i) {
        const MachineId_t machine_id = MachineId_t(i);
        const MachineInfo_t machine = Machine_GetInfo(machine_id);
        machines.push_back(machine_id);
        g_all_machines.push_back(machine_id);
        g_machine_sla_counts[machine_id] = {0, 0, 0, 0};

        if ((kSelectedAlgorithm == Algorithm::EECO || kSelectedAlgorithm == Algorithm::PMAPPER) &&
            i >= 4 && machine.s_state == S0) {
            g_sleeping_machines.insert(machine_id);
            Machine_SetState(machine_id, S3);
        }
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    (void)time;
    (void)vm_id;
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    (void)now;
    if (!TryAssignTask(task_id, true)) {
        QueueTask(task_id);
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    (void)now;
    DispatchPendingTasks();

    for (MachineId_t machine_id : g_all_machines) {
        TuneMachinePower(machine_id);
    }
    ManageIdleMachines();
}

void Scheduler::Shutdown(Time_t time) {
    (void)time;
    for (const auto &entry : g_vm_records) {
        const MachineInfo_t machine = Machine_GetInfo(entry.second.machine_id);
        if (machine.s_state == S0) {
            VM_Shutdown(entry.first);
        }
    }
    SimOutput("SimulationComplete(): algorithm=" + string(AlgorithmName()), 1);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    (void)now;
    auto task_vm_it = g_task_to_vm.find(task_id);
    auto task_machine_it = g_task_to_machine.find(task_id);
    if (task_vm_it == g_task_to_vm.end() || task_machine_it == g_task_to_machine.end()) {
        return;
    }

    const VMId_t vm_id = task_vm_it->second;
    const MachineId_t machine_id = task_machine_it->second;
    const TaskInfo_t task = GetTaskInfo(task_id);

    // The framework may have already removed the task from the VM's active list
    // by the time it calls TaskComplete, so guard before removing.
    const VMInfo_t vm_before = VM_GetInfo(vm_id);
    bool task_in_vm = false;
    for (const TaskId_t t : vm_before.active_tasks) {
        if (t == task_id) { task_in_vm = true; break; }
    }
    if (task_in_vm) {
        VM_RemoveTask(vm_id, task_id);
    }

    auto machine_sla_it = g_machine_sla_counts.find(machine_id);
    if (machine_sla_it != g_machine_sla_counts.end() && machine_sla_it->second[task.required_sla] > 0) {
        machine_sla_it->second[task.required_sla]--;
    }

    g_task_to_vm.erase(task_vm_it);
    g_task_to_machine.erase(task_machine_it);

    const VMInfo_t vm_after = VM_GetInfo(vm_id);
    if (vm_after.active_tasks.empty()) {
        DropEmptyVM(vm_id);
    }
    // Dispatch pending tasks immediately when a slot frees up, rather than
    // waiting for the next PeriodicCheck. Critical for GREEDY (WarmIdleTarget=0):
    // without a warm machine no StateChangeComplete wakeup events are generated,
    // so queued tasks would stall between completions until the next periodic tick.
    if (!g_pending_tasks.empty()) {
        DispatchPendingTasks();
    }
}

static Scheduler SchedulerInstance;

void InitScheduler() {
    SchedulerInstance.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SchedulerInstance.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SchedulerInstance.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    (void)time;
    g_machine_penalty[machine_id] += 3;
    SetMachinePerformance(machine_id, P0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SchedulerInstance.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    SchedulerInstance.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "Algorithm: " << AlgorithmName() << endl;
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
    g_waking_machines.erase(machine_id);
    g_sleeping_machines.erase(machine_id);
    DispatchPendingTasks();
}