/**
 * Jarvis Dashboard — Application Logic
 * Vanilla JS dashboard managing Knowledge Graph (vis-network) and Pipeline Monitor.
 */

const API_BASE = window.location.origin;
const WS_PROTOCOL = window.location.protocol === "https:" ? "wss:" : "ws:";
const WS_URL = `${WS_PROTOCOL}//${window.location.host}/ws?client=dashboard`;

const STAGE_META = {
	wake_word: { label: "WAKE", icon: "🎙️" },
	recording: { label: "RECORD", icon: "🔴" },
	transcription: { label: "STT", icon: "📝" },
	reasoning: { label: "LLM", icon: "🧠" },
	synthesis: { label: "TTS", icon: "🔊" }
};

const STAGE_ORDER = ["wake_word", "recording", "transcription", "reasoning", "synthesis"];

class DashboardApp {
	constructor () {
		this.ws = null;
		this.reconnectTimer = null;
		this.reconnectInterval = 3000;
		this.network = null;
		this.graphNodes = null;
		this.graphEdges = null;
		this.currentNodeData = {};
		this.pipelineStages = {};
		this.searchDebounce = null;

		STAGE_ORDER.forEach((id) => {
			this.pipelineStages[id] = { state: "idle", duration_ms: null };
		});

		this.initTabs();
		this.initKnowledgeGraph();
		this.initPipelineViz();
		this.initModals();
		this.initSearch();
		this.connectWebSocket();
		this.fetchGraphData();
		this.fetchPipelineHistory();
	}

	// ── WebSocket ──

	connectWebSocket () {
		if (this.ws && (this.ws.readyState === WebSocket.CONNECTING || this.ws.readyState === WebSocket.OPEN)) {
			return;
		}

		try {
			this.ws = new WebSocket(WS_URL);

			this.ws.onopen = () => {
				this.setWsStatus(true);
				if (this.reconnectTimer) {
					clearTimeout(this.reconnectTimer);
					this.reconnectTimer = null;
				}
			};

			this.ws.onmessage = (event) => {
				try {
					const msg = JSON.parse(event.data);
					this.handleWsMessage(msg);
				} catch (e) {
					console.error("WS parse error:", e);
				}
			};

			this.ws.onerror = () => {};

			this.ws.onclose = () => {
				this.setWsStatus(false);
				this.scheduleReconnect();
			};
		} catch {
			this.setWsStatus(false);
			this.scheduleReconnect();
		}
	}

	scheduleReconnect () {
		if (!this.reconnectTimer) {
			this.reconnectTimer = setTimeout(() => {
				this.reconnectTimer = null;
				this.connectWebSocket();
			}, this.reconnectInterval);
		}
	}

	setWsStatus (connected) {
		const dot = document.getElementById("wsDot");
		const label = document.getElementById("wsLabel");
		if (connected) {
			dot.classList.add("connected");
			label.textContent = "Connected";
		} else {
			dot.classList.remove("connected");
			label.textContent = "Disconnected";
		}
		label.setAttribute("aria-live", "polite");
	}

	handleWsMessage (msg) {
		if (msg.type === "status") {
			this.updatePipelineFromStatus(msg.data);
		} else if (msg.type === "response") {
			// New conversation added — refresh graph
			setTimeout(() => this.fetchGraphData(), 500);
		} else if (msg.type === "pipeline_state") {
			this.updatePipelineFromState(msg);
		} else if (msg.type === "knowledge_update") {
			this.fetchGraphData();
		} else if (msg.type === "provider") {
			const label = document.getElementById("providerLabel");
			const cloudActive = msg.provider === "gemini-live" && msg.status === "active";
			label.textContent = cloudActive ? "CLOUD" : "LOCAL";
			label.classList.toggle("is-cloud", cloudActive);
			label.classList.toggle("is-local", !cloudActive);
			label.setAttribute(
				"aria-label",
				cloudActive ? "Gemini cloud voice active" : "Local voice provider active"
			);
		}
	}

	// ── Tab Navigation ──

	initTabs () {
		document.querySelectorAll(".dash-tab").forEach((tab) => {
			tab.addEventListener("click", () => {
				document.querySelectorAll(".dash-tab").forEach((t) => t.classList.remove("active"));
				document.querySelectorAll(".dash-panel").forEach((p) => p.classList.remove("active"));
				tab.classList.add("active");
				const panelId = `panel${tab.dataset.tab.charAt(0).toUpperCase()}${tab.dataset.tab.slice(1)}`;
				const panel = document.getElementById(panelId);
				if (panel) panel.classList.add("active");

				// Resize vis-network when switching to knowledge tab
				if (tab.dataset.tab === "knowledge" && this.network) {
					setTimeout(() => this.network.fit(), 100);
				}
			});
		});
	}

	// ── Knowledge Graph ──

	initKnowledgeGraph () {
		const container = document.getElementById("graphCanvas");
		this.graphNodes = new vis.DataSet([]);
		this.graphEdges = new vis.DataSet([]);

		const options = {
			nodes: {
				shape: "dot",
				size: 18,
				font: {
					color: "#ECEFF1",
					size: 12,
					face: "Inter, sans-serif"
				},
				borderWidth: 2,
				shadow: {
					enabled: true,
					color: "rgba(0, 229, 255, 0.2)",
					size: 10
				}
			},
			edges: {
				color: {
					color: "rgba(255, 255, 255, 0.15)",
					highlight: "#00E5FF",
					hover: "#00E5FF"
				},
				width: 1,
				smooth: {
					type: "continuous",
					roundness: 0.3
				}
			},
			physics: {
				solver: "forceAtlas2Based",
				forceAtlas2Based: {
					gravitationalConstant: -40,
					centralGravity: 0.008,
					springLength: 120,
					springConstant: 0.03,
					damping: 0.4
				},
				stabilization: { iterations: 100 }
			},
			interaction: {
				hover: true,
				tooltipDelay: 200,
				zoomView: true,
				dragView: true
			},
			layout: {
				improvedLayout: true
			}
		};

		this.network = new vis.Network(container, { nodes: this.graphNodes, edges: this.graphEdges }, options);

		this.network.on("click", (params) => {
			if (params.nodes.length > 0) {
				this.showNodeDetail(params.nodes[0]);
			} else {
				this.hideNodeDetail();
			}
		});

		// Close detail button
		document.getElementById("btnCloseDetail").addEventListener("click", () => this.hideNodeDetail());
		document.getElementById("btnRefreshGraph").addEventListener("click", () => this.fetchGraphData());
	}

	async fetchGraphData () {
		try {
			const res = await fetch(`${API_BASE}/api/knowledge/graph`);
			if (!res.ok) return;
			const data = await res.json();

			// Update vis-network datasets
			const visNodes = data.nodes.map((n) => ({
				id: n.id,
				label: this.truncate(n.title, 25),
				title: n.title,
				color: {
					background: n.type === "conversation" ? "rgba(0, 229, 255, 0.2)" : "rgba(179, 136, 255, 0.2)",
					border: n.type === "conversation" ? "#00E5FF" : "#B388FF",
					highlight: {
						background: n.type === "conversation" ? "rgba(0, 229, 255, 0.4)" : "rgba(179, 136, 255, 0.4)",
						border: n.type === "conversation" ? "#00E5FF" : "#B388FF"
					}
				}
			}));

			const visEdges = data.edges.map((e) => ({
				id: e.id,
				from: e.source,
				to: e.target
			}));

			this.graphNodes.clear();
			this.graphNodes.add(visNodes);
			this.graphEdges.clear();
			this.graphEdges.add(visEdges);

			// Cache node data for detail view
			this.currentNodeData = {};
			data.nodes.forEach((n) => { this.currentNodeData[n.id] = n; });

			this.fetchStats();
		} catch (e) {
			console.error("Failed to fetch graph data:", e);
		}
	}

	async fetchStats () {
		try {
			const res = await fetch(`${API_BASE}/api/knowledge/stats`);
			if (!res.ok) return;
			const stats = await res.json();
			document.getElementById("statTotal").textContent = stats.total_nodes || 0;
			document.getElementById("statConvos").textContent = stats.conversations || 0;
			document.getElementById("statNotes").textContent = stats.notes || 0;
			document.getElementById("statEdges").textContent = stats.edges || 0;
		} catch (e) {
			console.error("Failed to fetch stats:", e);
		}
	}

	async showNodeDetail (nodeId) {
		let nodeData = this.currentNodeData[nodeId];

		// Fetch full node data with neighbors if not cached
		if (!nodeData || !nodeData.neighbors) {
			try {
				const res = await fetch(`${API_BASE}/api/knowledge/nodes/${nodeId}`);
				if (res.ok) {
					nodeData = await res.json();
					this.currentNodeData[nodeId] = nodeData;
				}
			} catch (e) {
				console.error("Failed to fetch node detail:", e);
				return;
			}
		}

		if (!nodeData) return;

		const panel = document.getElementById("nodeDetail");
		document.getElementById("detailTitle").textContent = nodeData.title;

		const badge = document.getElementById("detailType");
		badge.textContent = nodeData.type;
		badge.className = `dash-badge ${nodeData.type === "note" ? "note" : ""}`;

		document.getElementById("detailTime").textContent = nodeData.timestamp
			? new Date(nodeData.timestamp * 1000).toLocaleString()
			: "";

		document.getElementById("detailContent").textContent = nodeData.content || "";

		// Tags
		const tagsEl = document.getElementById("detailTags");
		tagsEl.innerHTML = "";
		if (nodeData.tags && nodeData.tags.length > 0) {
			nodeData.tags.forEach((tag) => {
				const tagEl = document.createElement("span");
				tagEl.className = "dash-tag";
				tagEl.textContent = tag;
				tagsEl.appendChild(tagEl);
			});
		}

		// Neighbors
		const neighborsEl = document.getElementById("detailNeighbors");
		neighborsEl.innerHTML = "";
		if (nodeData.neighbors && nodeData.neighbors.length > 0) {
			const heading = document.createElement("h4");
			heading.textContent = "Linked Nodes";
			neighborsEl.appendChild(heading);
			nodeData.neighbors.forEach((n) => {
				const item = document.createElement("div");
				item.className = "dash-neighbor-item";
				item.textContent = `${n.relation} → ${n.title}`;
				item.addEventListener("click", () => {
					this.network.selectNodes([n.id]);
					this.showNodeDetail(n.id);
				});
				neighborsEl.appendChild(item);
			});
		}

		panel.style.display = "block";
	}

	hideNodeDetail () {
		document.getElementById("nodeDetail").style.display = "none";
	}

	// ── Search ──

	initSearch () {
		const input = document.getElementById("searchInput");
		input.addEventListener("input", () => {
			clearTimeout(this.searchDebounce);
			this.searchDebounce = setTimeout(() => {
				const q = input.value.trim();
				if (q.length >= 2) {
					this.searchNodes(q);
				} else {
					document.getElementById("searchResults").innerHTML = "";
				}
			}, 300);
		});
	}

	async searchNodes (query) {
		try {
			const res = await fetch(`${API_BASE}/api/knowledge/search?q=${encodeURIComponent(query)}`);
			if (!res.ok) return;
			const results = await res.json();

			const container = document.getElementById("searchResults");
			container.innerHTML = "";

			if (results.length === 0) {
				container.innerHTML = "<div class=\"dash-empty\" style=\"padding: 8px; font-size: 0.75rem;\">No results found</div>";
				return;
			}

			results.forEach((node) => {
				const item = document.createElement("div");
				item.className = "dash-search-item";
				item.innerHTML = `
					<div class="dash-search-item-title">${this.escapeHtml(node.title)}</div>
					<div class="dash-search-item-type">${node.type}</div>
				`;
				item.addEventListener("click", () => {
					this.network.selectNodes([node.id]);
					this.network.focus(node.id, { scale: 1.2, animation: true });
					this.showNodeDetail(node.id);
				});
				container.appendChild(item);
			});
		} catch (e) {
			console.error("Search failed:", e);
		}
	}

	// ── Note Modal ──

	initModals () {
		const modal = document.getElementById("noteModal");
		const form = document.getElementById("noteForm");

		document.getElementById("btnNewNote").addEventListener("click", () => {
			modal.style.display = "flex";
		});

		document.getElementById("btnCloseModal").addEventListener("click", () => {
			modal.style.display = "none";
		});

		document.getElementById("btnCancelNote").addEventListener("click", () => {
			modal.style.display = "none";
		});

		modal.addEventListener("click", (e) => {
			if (e.target === modal) modal.style.display = "none";
		});

		form.addEventListener("submit", async (e) => {
			e.preventDefault();
			const title = document.getElementById("noteTitle").value.trim();
			const content = document.getElementById("noteContent").value.trim();
			const tagsRaw = document.getElementById("noteTags").value.trim();
			const tags = tagsRaw ? tagsRaw.split(",").map((t) => t.trim()).filter(Boolean) : [];

			try {
				const res = await fetch(`${API_BASE}/api/knowledge/notes`, {
					method: "POST",
					headers: { "Content-Type": "application/json" },
					body: JSON.stringify({ title, content, tags })
				});

				if (res.ok) {
					modal.style.display = "none";
					form.reset();
					this.fetchGraphData();
				}
			} catch (e) {
				console.error("Failed to create note:", e);
			}
		});
	}

	// ── Pipeline Visualization ──

	initPipelineViz () {
		this.renderPipelineGraph();
	}

	renderPipelineGraph () {
		const container = document.getElementById("pipelineGraph");
		container.innerHTML = "";

		STAGE_ORDER.forEach((id, idx) => {
			const stageData = this.pipelineStages[id] || { state: "idle" };
			const meta = STAGE_META[id];

			const node = document.createElement("div");
			node.className = `dash-pn ${stageData.state}`;
			node.id = `pn-${id}`;

			const icon = document.createElement("div");
			icon.className = "dash-pn-icon";
			icon.textContent = meta.icon;
			node.appendChild(icon);

			const label = document.createElement("div");
			label.className = "dash-pn-label";
			label.textContent = meta.label;
			node.appendChild(label);

			if (stageData.duration_ms !== null && stageData.duration_ms !== undefined) {
				const timing = document.createElement("div");
				timing.className = "dash-pn-timing";
				timing.textContent = `${Math.round(stageData.duration_ms)}ms`;
				node.appendChild(timing);
			}

			container.appendChild(node);

			// Arrow
			if (idx < STAGE_ORDER.length - 1) {
				const arrow = document.createElement("div");
				arrow.className = `dash-pa ${(stageData.state === "completed" || stageData.state === "active") ? "active" : ""}`;

				const line = document.createElement("div");
				line.className = "dash-pa-line";
				arrow.appendChild(line);

				const head = document.createElement("div");
				head.className = "dash-pa-head";
				arrow.appendChild(head);

				container.appendChild(arrow);
			}
		});
	}

	updatePipelineFromStatus (statusText) {
		const STATUS_MAP = {
			"Listening...": "recording",
			"Transcribing...": "transcription",
			"Thinking...": "reasoning",
			"Speaking...": "synthesis"
		};

		if (statusText === "Idle" || statusText === "Connected") {
			STAGE_ORDER.forEach((id) => {
				this.pipelineStages[id] = { state: "idle", duration_ms: null };
			});
		} else if (statusText === "Error") {
			STAGE_ORDER.forEach((id) => {
				if (this.pipelineStages[id].state === "active") {
					this.pipelineStages[id].state = "error";
				}
			});
		} else {
			const activeStage = STATUS_MAP[statusText];
			if (activeStage) {
				const activeIdx = STAGE_ORDER.indexOf(activeStage);
				STAGE_ORDER.forEach((id, idx) => {
					if (idx < activeIdx) {
						this.pipelineStages[id].state = "completed";
					} else if (idx === activeIdx) {
						this.pipelineStages[id].state = "active";
					} else {
						this.pipelineStages[id].state = "idle";
					}
				});
				if (activeIdx >= 0) {
					this.pipelineStages.wake_word.state = "completed";
				}
			}
		}

		this.renderPipelineGraph();

		if (statusText === "Idle") {
			this.fetchPipelineHistory();
		}
	}

	updatePipelineFromState (stateMsg) {
		if (stateMsg.stages && Array.isArray(stateMsg.stages)) {
			stateMsg.stages.forEach((s) => {
				this.pipelineStages[s.id] = {
					state: s.state || "idle",
					duration_ms: s.duration_ms || null
				};
			});
		}
		this.renderPipelineGraph();
	}

	async fetchPipelineHistory () {
		try {
			const res = await fetch(`${API_BASE}/api/pipeline/history`);
			if (!res.ok) return;
			const history = await res.json();
			this.renderHistoryTable(history);
		} catch (e) {
			console.error("Failed to fetch pipeline history:", e);
		}
	}

	renderHistoryTable (history) {
		const tbody = document.getElementById("historyBody");
		if (!history || history.length === 0) {
			tbody.innerHTML = "<tr><td colspan=\"8\" class=\"dash-empty\">No cycle history yet</td></tr>";
			return;
		}

		tbody.innerHTML = "";
		history.forEach((cycle, idx) => {
			const row = document.createElement("tr");
			const time = cycle.started_at ? new Date(cycle.started_at * 1000).toLocaleTimeString() : "—";

			const getDuration = (stageId) => {
				const stage = cycle.stages && cycle.stages[stageId];
				if (stage && stage.duration_ms !== null && stage.duration_ms !== undefined) {
					return `${Math.round(stage.duration_ms)}ms`;
				}
				return "—";
			};

			row.innerHTML = `
				<td>#${history.length - idx}</td>
				<td>${time}</td>
				<td>${getDuration("wake_word")}</td>
				<td>${getDuration("recording")}</td>
				<td>${getDuration("transcription")}</td>
				<td>${getDuration("reasoning")}</td>
				<td>${getDuration("synthesis")}</td>
				<td style="color: var(--accent-cyan); font-weight: 600;">${cycle.total_duration_ms ? `${Math.round(cycle.total_duration_ms)}ms` : "—"}</td>
			`;
			tbody.appendChild(row);
		});
	}

	// ── Utilities ──

	truncate (str, max) {
		if (!str) return "";
		return str.length > max ? `${str.substring(0, max)}…` : str;
	}

	escapeHtml (str) {
		if (!str) return "";
		return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
			.replace(/"/g, "&quot;")
			.replace(/'/g, "&#039;");
	}
}

// ── Initialize ──
document.addEventListener("DOMContentLoaded", () => {
	window.dashboardApp = new DashboardApp();
});
