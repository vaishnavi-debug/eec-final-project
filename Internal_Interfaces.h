//
//  Internal_Interfaces.h
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 11/3/24.
//

#ifndef Internal_Interfaces_h
#define Internal_Interfaces_h

#include <string>
#include <vector>

#include "SimTypes.h"

// CPU Interface
extern CPUId_t AddCPU(CPUType_t cpu, vector<unsigned>cpu_dynamic, vector<unsigned> cpu_static, vector<unsigned> performance, bool gpu_flag);
extern CPUType_t CPU_GetType(CPUId_t cpu_id);
extern long long GetConsumedEnergy(CPUId_t cpu_id);
void CPU_AbortTask(CPUId_t cpu_id);
Time_t CPU_RunTask(CPUId_t cpu_id, TaskId_t task_id);
void CPU_StopTask(CPUId_t cpu_id);

// Initializer interface
extern void Init(string filename);

// Internal Machine Interface
extern MachineId_t Machine_Add(unsigned memory, vector<unsigned> machine_power);
extern void Machine_Add(u_int mem, u_int cores, vector<u_int> & s_states, vector<u_int> & c_states, vector<u_int> & p_states, vector<u_int> & mips, bool gpu, CPUType_t cpu);
extern void Machine_AttachCPU(MachineId_t machine_id, CPUId_t cpu_id);
extern void Machine_AttachVM(MachineId_t machine_id, VMId_t vm_id);
extern void Machine_CompleteTask(MachineId_t machine_id, unsigned core_id);
extern bool Machine_CheckMemoryOverflow(MachineId_t machine);
extern void Machine_DetachVM(MachineId_t machine_id, VMId_t vm_id);
extern void Machine_AttachTask(MachineId_t machine_id, TaskId_t task_id, VMId_t vm_id);
extern void Machine_HandleTimer(Time_t time);
extern void Machine_MigrateVM(VMId_t vm_id, MachineId_t current, MachineId_t next);

// Internal Simulator Interface
extern void StartSimulation();
extern void ScheduleMigrationCompletion(Time_t time, VMId_t vm_id);
extern void ScheduleNewTask(Time_t time, TaskId_t task_id);
extern void ScheduleTaskCompletion(Time_t time, TaskId_t task_id);
extern void ScheduleTaskCompletion(Time_t time, MachineId_t machine_id, unsigned core_id);
extern void ScheduleTimer(Time_t time);

// Internal task Interface
extern TaskId_t AddTask(uint64_t inst, Time_t arr, Time_t trgt, VMType_t vm, SLAType_t sla, CPUType_t cpu, bool gpu, unsigned mem, TaskClass_t task_class);
extern void CompleteTask(TaskId_t task_id);
extern unsigned GetActiveTasks();
extern uint64_t GetRemainingInstructions(TaskId_t task_id);
extern void SetRemainingInstructions(TaskId_t task_id, uint64_t instructions);

// Internal VM Interface
extern bool VM_IsPendingMigration(VMId_t vm_id);
extern void VM_MigrationCompleted(VMId_t vm_id);
extern void VM_MigrationStarted(VMId_t vm_id);

#endif /* Internal_Interfaces_h */
