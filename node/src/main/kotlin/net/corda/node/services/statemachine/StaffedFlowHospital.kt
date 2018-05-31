package net.corda.node.services.statemachine

import net.corda.core.flows.StateMachineRunId
import net.corda.core.internal.VisibleForTesting
import net.corda.core.utilities.loggerFor
import net.corda.node.services.FinalityHandler
import org.hibernate.exception.ConstraintViolationException
import java.sql.SQLException
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * This hospital consults "staff" to see if they can automatically diagnose and treat flows.
 */
object StaffedFlowHospital : FlowHospital {
    private val log = loggerFor<StaffedFlowHospital>()

    private val staff = listOf(DeadlockNurse, DuplicateInsertSpecialist, FinalityDoctor)

    @VisibleForTesting
    internal val patients = ConcurrentHashMap<StateMachineRunId, MedicalHistory>()

    class MedicalHistory {
        val records: MutableList<Record> = mutableListOf()

        sealed class Record {
            abstract val suspendCount: Int

            data class Admitted(val at: Instant, override val suspendCount: Int) : Record()
            data class Discharged(val at: Instant, override val suspendCount: Int, val by: List<Staff>, val errors: List<Throwable>) : Record()
            data class Observation(val at: Instant, override val suspendCount: Int, val by: List<Staff>, val errors: List<Throwable>) : Record()
        }

        fun notDischargedForTheSameThingMoreThan(max: Int, by: Staff): Boolean {
            val lastAdmittanceSuspendCount = (records.last() as MedicalHistory.Record.Admitted).suspendCount
            return records
                    .filterIsInstance<MedicalHistory.Record.Discharged>()
                    .count { by in it.by && it.suspendCount == lastAdmittanceSuspendCount } <= max
        }

        override fun toString(): String = "${this.javaClass.simpleName}(records = $records)"
    }

    override fun flowErrored(flowFiber: FlowFiber, currentState: StateMachineState, errors: List<Throwable>) {
        log.info("Flow ${flowFiber.id} admitted to hospital in state $currentState")
        val medicalHistory = patients.computeIfAbsent(flowFiber.id) { MedicalHistory() }
        medicalHistory.records += MedicalHistory.Record.Admitted(Instant.now(), currentState.checkpoint.numberOfSuspends)

        val diagnoses: Map<Diagnosis, List<Staff>> = staff.groupBy { it.consult(flowFiber, currentState, errors, medicalHistory) }

        diagnoses[Diagnosis.DISCHARGE]?.let {
            log.info("Flow ${flowFiber.id} error discharged from hospital by $it")
            medicalHistory.records += MedicalHistory.Record.Discharged(Instant.now(), currentState.checkpoint.numberOfSuspends, it, errors)
            flowFiber.scheduleEvent(Event.RetryFlowFromSafePoint)
            return
        }

        diagnoses[Diagnosis.OVERNIGHT_OBSERVATION]?.let {
            log.info("Flow ${flowFiber.id} error kept for overnight observation by $it")
            medicalHistory.records += MedicalHistory.Record.Observation(Instant.now(), currentState.checkpoint.numberOfSuspends, it, errors)
            // No need to do anything. The flow will automatically retry from its checkpoint on node restart
            return
        }

        // None of the staff care for these errors so we let them propagate
        flowFiber.scheduleEvent(Event.StartErrorPropagation)
    }

    // It's okay for flows to be cleaned... we fix them now!
    override fun flowCleaned(flowFiber: FlowFiber) {}

    override fun flowRemoved(flowFiber: FlowFiber) {
        patients.remove(flowFiber.id)
    }

    enum class Diagnosis {
        /**
         * Retry from last safe point.
         */
        DISCHARGE,

        /**
         * Park and retry on node restart.
         */
        OVERNIGHT_OBSERVATION,

        /**
         * Please try another member of staff.
         */
        NOT_MY_SPECIALTY
    }

    interface Staff {
        fun consult(flowFiber: FlowFiber, currentState: StateMachineState, errors: List<Throwable>, history: MedicalHistory): Diagnosis
    }

    /**
     * SQL Deadlock detection.
     */
    object DeadlockNurse : Staff {
        override fun consult(flowFiber: FlowFiber, currentState: StateMachineState, errors: List<Throwable>, history: MedicalHistory): Diagnosis {
            return if (errors.any(::mentionsDeadlock)) {
                Diagnosis.DISCHARGE
            } else {
                Diagnosis.NOT_MY_SPECIALTY
            }
        }

        private fun mentionsDeadlock(exception: Throwable?): Boolean {
            return exception != null && (exception is SQLException && ((exception.message?.toLowerCase()?.contains("deadlock")
                    ?: false)) || mentionsDeadlock(exception.cause))
        }
    }

    /**
     * Primary key violation detection for duplicate inserts.  Will detect other constraint violations too.
     */
    object DuplicateInsertSpecialist : Staff {
        override fun consult(flowFiber: FlowFiber, currentState: StateMachineState, errors: List<Throwable>, history: MedicalHistory): Diagnosis {
            return if (errors.any(::mentionsConstraintViolation) && history.notDischargedForTheSameThingMoreThan(3, this)) {
                Diagnosis.DISCHARGE
            } else {
                Diagnosis.NOT_MY_SPECIALTY
            }
        }

        private fun mentionsConstraintViolation(exception: Throwable?): Boolean {
            return exception != null && (exception is ConstraintViolationException || mentionsConstraintViolation(exception.cause))
        }
    }

    object FinalityDoctor : Staff {
        override fun consult(flowFiber: FlowFiber, currentState: StateMachineState, errors: List<Throwable>, history: MedicalHistory): Diagnosis {
            return if (currentState.flowLogic is FinalityHandler) Diagnosis.OVERNIGHT_OBSERVATION else Diagnosis.NOT_MY_SPECIALTY
        }
    }
}