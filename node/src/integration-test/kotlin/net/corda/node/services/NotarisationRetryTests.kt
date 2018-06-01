package net.corda.node.services

import co.paralleluniverse.fibers.Suspendable
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import net.corda.core.contracts.AlwaysAcceptAttachmentConstraint
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.TransactionSignature
import net.corda.core.crypto.isFulfilledBy
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.FlowSession
import net.corda.core.flows.NotarisationPayload
import net.corda.core.flows.NotaryFlow
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.FlowIORequest
import net.corda.core.internal.ResolveTransactionsFlow
import net.corda.core.internal.notary.NotaryServiceFlow
import net.corda.core.internal.notary.TrustedAuthorityNotaryService
import net.corda.core.internal.notary.UniquenessProvider
import net.corda.core.node.AppServiceHub
import net.corda.core.node.NotaryInfo
import net.corda.core.node.services.CordaService
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.seconds
import net.corda.node.internal.StartedNode
import net.corda.node.services.config.NotaryConfig
import net.corda.nodeapi.internal.DevIdentityGenerator
import net.corda.nodeapi.internal.network.NetworkParametersCopier
import net.corda.testing.common.internal.testNetworkParameters
import net.corda.testing.contracts.DummyContract
import net.corda.testing.core.dummyCommand
import net.corda.testing.core.singleIdentity
import net.corda.testing.internal.LogHelper
import net.corda.testing.node.InMemoryMessagingNetwork
import net.corda.testing.node.MockNetworkParameters
import net.corda.testing.node.internal.InternalMockNetwork
import net.corda.testing.node.internal.InternalMockNodeParameters
import net.corda.testing.node.internal.startFlow
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.security.PublicKey
import java.util.concurrent.atomic.AtomicInteger

class NotarisationRetryTests {
    private lateinit var mockNet: InternalMockNetwork
    private lateinit var notary: Party
    private lateinit var node: StartedNode<InternalMockNetwork.MockNode>
    private val CLUSTER_SIZE = 3

    init {
        LogHelper.setLevel("+net.corda.flow", "+net.corda.testing.node", "+net.corda.node.services.messaging")
    }

    @Before
    fun before() {

        mockNet = InternalMockNetwork(
                listOf("net.corda.testing.contracts", "net.corda.node.services"),
                MockNetworkParameters().withServicePeerAllocationStrategy(InMemoryMessagingNetwork.ServicePeerAllocationStrategy.RoundRobin()),
                threadPerNode = true
        )
        val started = startClusterAndNode(mockNet)
        notary = started.first
        node = started.second
        TestNotaryService.requestsReceived = AtomicInteger(0)
    }

    @After
    fun stopNodes() {
        mockNet.stopNodes()
    }

    private fun startClusterAndNode(mockNet: InternalMockNetwork): Pair<Party, StartedNode<InternalMockNetwork.MockNode>> {
        val replicaIds = (0 until CLUSTER_SIZE)
        val notaryIdentity = DevIdentityGenerator.generateDistributedNotaryCompositeIdentity(
                replicaIds.map { mockNet.baseDirectory(mockNet.nextNodeId + it) },
                CordaX500Name("Custom", "Zurich", "CH"))

        val networkParameters = NetworkParametersCopier(testNetworkParameters(listOf(NotaryInfo(notaryIdentity, true))))

        val notaryConfig = mock<NotaryConfig> {
            whenever(it.custom).thenReturn(true)
            whenever(it.isClusterConfig).thenReturn(true)
            whenever(it.validating).thenReturn(true)
        }

        val nodes = (0 until CLUSTER_SIZE).map {
            mockNet.createUnstartedNode(InternalMockNodeParameters(configOverrides = {
                val notary = notaryConfig
                doReturn(notary).whenever(it).notary
            }))
        } + mockNet.createUnstartedNode(InternalMockNodeParameters(legalName = CordaX500Name("Alice", "AliceCorp", "GB")))

        // MockNetwork doesn't support notary clusters, so we create all the nodes we need unstarted, and then install the
        // network-parameters in their directories before they're started.
        val node = nodes.map { node ->
            networkParameters.install(mockNet.baseDirectory(node.id))
            node.start()
        }.last()

        return Pair(notaryIdentity, node)
    }

    @Test
    fun `notarise issue tx with time-window`() {
        node.run {
            val issueTx = signInitialTransaction(notary) {
                setTimeWindow(services.clock.instant(), 30.seconds)
                addOutputState(DummyContract.SingleOwnerState(owner = info.singleIdentity()), DummyContract.PROGRAM_ID, AlwaysAcceptAttachmentConstraint)
            }
            val resultFuture = services.startFlow(NotaryFlow.Client(issueTx)).resultFuture
            val signatures = resultFuture.get()
            verifySignatures(signatures, issueTx.id)
        }
    }

    private fun verifySignatures(signatures: List<TransactionSignature>, txId: SecureHash) {
        notary.owningKey.isFulfilledBy(signatures.map { it.by })
        signatures.forEach { it.verify(txId) }
    }

    private fun StartedNode<InternalMockNetwork.MockNode>.signInitialTransaction(notary: Party, block: TransactionBuilder.() -> Any?): SignedTransaction {
        return services.signInitialTransaction(
                TransactionBuilder(notary).apply {
                    addCommand(dummyCommand(services.myInfo.singleIdentity().owningKey))
                    block()
                }
        )
    }

    @CordaService
    private class TestNotaryService(override val services: AppServiceHub, override val notaryIdentityKey: PublicKey) : TrustedAuthorityNotaryService() {
        companion object {
            lateinit var requestsReceived: AtomicInteger
        }

        override val uniquenessProvider = mock<UniquenessProvider>()
        override fun createServiceFlow(otherPartySession: FlowSession): FlowLogic<Void?> = TestNotaryFlow(otherPartySession, this)
        override fun start() {}
        override fun stop() {}
    }

    private class TestNotaryFlow(otherSide: FlowSession, service: TestNotaryService) : NotaryServiceFlow(otherSide, service) {
        @Suspendable
        override fun validateRequest(requestPayload: NotarisationPayload): TransactionParts {
            val myIdentity =  serviceHub.myInfo.legalIdentities.first()
            println("$myIdentity: Received a request from ${otherSideSession.counterparty.name}")
            val stx = requestPayload.signedTransaction
            subFlow(ResolveTransactionsFlow(stx, otherSideSession))

            if (TestNotaryService.requestsReceived.getAndIncrement() == 0) {
                // ignore request / crash
                println("$myIdentity - ignoring")
                // Waiting forever
                stateMachine.suspend(FlowIORequest.WaitForLedgerCommit(SecureHash.randomSHA256()), false)
            } else {
                // all good
                println("$myIdentity - processing")
            }
            return TransactionParts(stx.id, stx.inputs, stx.tx.timeWindow, stx.notary)
        }
    }
}