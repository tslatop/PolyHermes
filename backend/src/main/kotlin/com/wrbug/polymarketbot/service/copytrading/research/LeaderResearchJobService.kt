package com.wrbug.polymarketbot.service.copytrading.research

import com.wrbug.polymarketbot.entity.LeaderResearchRun
import com.wrbug.polymarketbot.enums.LeaderResearchEventType
import com.wrbug.polymarketbot.enums.LeaderResearchRunStatus
import com.wrbug.polymarketbot.enums.LeaderResearchState
import com.wrbug.polymarketbot.enums.LeaderResearchTriggerType
import com.wrbug.polymarketbot.repository.LeaderActivityEventRepository
import com.wrbug.polymarketbot.repository.LeaderResearchCandidateRepository
import com.wrbug.polymarketbot.repository.LeaderResearchRunRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.concurrent.atomic.AtomicBoolean

@Service
class LeaderResearchJobService(
    private val runRepository: LeaderResearchRunRepository,
    private val activityEventRepository: LeaderActivityEventRepository,
    private val candidateRepository: LeaderResearchCandidateRepository,
    private val sourceService: LeaderResearchSourceService,
    private val paperTradingService: LeaderPaperTradingService,
    private val scoringService: LeaderResearchScoringService,
    private val stateMachine: LeaderResearchStateMachine,
    private val eventService: LeaderResearchEventService,
    @Value("\${leader.research.enabled:false}") private val scheduledEnabled: Boolean
) {
    private val logger = LoggerFactory.getLogger(LeaderResearchJobService::class.java)
    private val running = AtomicBoolean(false)

    @Scheduled(fixedDelayString = "\${leader.research.fixed-delay-ms:900000}")
    fun scheduledRun() {
        if (!scheduledEnabled) return
        runOnce(dryRun = false, triggerType = LeaderResearchTriggerType.SCHEDULED)
    }

    @Transactional
    fun runOnce(dryRun: Boolean, triggerType: LeaderResearchTriggerType = LeaderResearchTriggerType.MANUAL): LeaderResearchRun {
        if (!running.compareAndSet(false, true)) {
            val now = System.currentTimeMillis()
            val skipped = runRepository.save(
                LeaderResearchRun(
                    status = LeaderResearchRunStatus.SKIPPED,
                    triggerType = triggerType,
                    dryRun = dryRun,
                    startedAt = now,
                    finishedAt = now,
                    durationMs = 0,
                    skippedReason = "another_run_in_progress",
                    createdAt = now,
                    updatedAt = now
                )
            )
            eventService.record(
                type = LeaderResearchEventType.RUN_SKIPPED,
                runId = skipped.id,
                reason = "Skipped because another research run is in progress"
            )
            return skipped
        }

        val startedAt = System.currentTimeMillis()
        var run = runRepository.save(
            LeaderResearchRun(
                status = LeaderResearchRunStatus.RUNNING,
                triggerType = triggerType,
                dryRun = dryRun,
                startedAt = startedAt,
                createdAt = startedAt,
                updatedAt = startedAt
            )
        )
        eventService.record(
            type = LeaderResearchEventType.RUN_STARTED,
            runId = run.id,
            reason = "Leader research run started"
        )

        return try {
            val isPreview = dryRun || triggerType == LeaderResearchTriggerType.PREVIEW
            val sourceResults = if (isPreview) sourceService.previewCandidates() else sourceService.discoverCandidates(run.id)
            if (!isPreview) {
                scoringService.scoreAll(run.id)
                stateMachine.advanceAll(run.id)
                paperTradingService.processPaperCandidates(run.id)
                scoringService.scoreAll(run.id)
                stateMachine.advanceAll(run.id)
            }
            val now = System.currentTimeMillis()
            val sourceCounts = sourceResults.joinToString(",", prefix = "{", postfix = "}") {
                "\"${it.sourceType.name}\":${it.candidates.size}"
            }
            val candidateCounts = LeaderResearchState.values().joinToString(",", prefix = "{", postfix = "}") { state ->
                "\"${state.name}\":${candidateRepository.countByResearchState(state)}"
            }
            val lastEventCursor = activityEventRepository.findTopByOrderByEventTimeDesc()
                ?.let { "${it.eventTime}:${it.stableEventKey}" }
            val hasSourceProblems = sourceResults.any { it.status.name == "FAILURE" || it.status.name == "DEGRADED" }
            run = runRepository.save(
                run.copy(
                    status = if (hasSourceProblems) LeaderResearchRunStatus.PARTIAL_FAILURE else LeaderResearchRunStatus.SUCCESS,
                    finishedAt = now,
                    durationMs = now - startedAt,
                    sourceCountsJson = sourceCounts,
                    candidateCountsJson = candidateCounts,
                    lastEventCursor = lastEventCursor,
                    partialFailure = hasSourceProblems,
                    updatedAt = now
                )
            )
            eventService.record(
                type = LeaderResearchEventType.RUN_COMPLETED,
                runId = run.id,
                reason = "Leader research run completed",
                payloadSummary = "sourceCounts=$sourceCounts candidateCounts=$candidateCounts"
            )
            run
        } catch (e: Exception) {
            logger.error("Leader research run failed", e)
            val now = System.currentTimeMillis()
            runRepository.save(
                run.copy(
                    status = LeaderResearchRunStatus.FAILED,
                    finishedAt = now,
                    durationMs = now - startedAt,
                    errorClass = e::class.java.simpleName,
                    errorMessage = e.message,
                    updatedAt = now
                )
            ).also {
                eventService.record(
                    type = LeaderResearchEventType.RUN_FAILED,
                    runId = it.id,
                    reason = e.message,
                    payloadSummary = e::class.java.name
                )
            }
        } finally {
            running.set(false)
        }
    }
}
