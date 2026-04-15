(function () {
  const errorClassName = "is-error";
  const collapsedClassName = "is-collapsed";
  const collapsibleToggleSelector = "[data-collapsible-toggle]";
  const directionBothValue = "both";
  const directionName = "direction";
  const histogramAxisTimestampSelector = ".histogram-axis-label[data-timestamp-ns][data-span-width-ns]";
  const histogramBarSelector = ".histogram-bar";
  const graphSceneSelector = ".graph-scene";
  const granularityName = "granularity";
  const histogramTooltipClassName = "histogram-tooltip";
  const histogramTooltipOffsetPx = 14;
  const loadingClassName = "is-loading";
  const loadingMessage = "Loading data...";
  const maxGraphScale = 6;
  const minGraphScale = 0.75;
  const nodeLimitAutoValue = "auto";
  const oneDayNs = 24n * 3600n * 1000000000n;
  const entityActionMaxRangeNs = 7n * oneDayNs;
  const oneHourNs = 3600n * 1000000000n;
  const presetAllValue = "all";
  const presetBehavior = "preset";
  const presetDayLegacyValue = "24h";
  const presetDayValue = "1d";
  const presetHourValue = "1h";
  const presetMonthValue = "30d";
  const presetWeekValue = "7d";
  const reloadCheckIntervalMs = 1000;
  const sceneLabelPaddingPx = 8;
  const searchBehavior = "search";
  const statusErrorMessage = "Request failed.";
  const statusSelector = "#loading-indicator";
  const timestampSelector = "time[data-timestamp-ns]";
  const metricDNSLookupsValue = "dns_lookups";
  const millisecondsPerDay = 86400000;
  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const nanosecondsPerMillisecond = 1000000n;
  const weekdayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const versionPath = "/version";

  let hotReloadInitialized = false;
  let searchTimeoutId = null;

  function initialize(root) {
    const form = root.querySelector("#filters-form");
    if (form && !form.dataset.initialized) {
      form.dataset.initialized = "true";
      bindForm(form);
    }

    const histogram = root.querySelector("#histogram");
    if (histogram && !histogram.dataset.initialized) {
      histogram.dataset.initialized = "true";
      bindHistogram(histogram);
    }

    const graphCanvas = root.querySelector("#graph-canvas");
    if (graphCanvas && !graphCanvas.dataset.initialized) {
      graphCanvas.dataset.initialized = "true";
      bindGraph(graphCanvas);
    }

    bindSectionToggles(root);
    localizeTimestamps(root);
  }

  function localizeTimestamps(root) {
    const now = new Date();
    for (const timestamp of root.querySelectorAll(timestampSelector)) {
      const label = formatTimestampNs(timestamp.dataset.timestampNs, now);
      timestamp.textContent = label;
      timestamp.setAttribute("title", label);
    }

    for (const bar of root.querySelectorAll(histogramBarSelector)) {
      const fromLabel = formatTimestampNs(bar.dataset.fromNs, now);
      const toLabel = formatTimestampNs(bar.dataset.toNs, now);
      const valueLabel = bar.dataset.valueLabel || "";
      bar.dataset.fromLabel = fromLabel;
      bar.dataset.toLabel = toLabel;

      const title = bar.querySelector("title");
      if (title) {
        title.textContent = `${fromLabel} - ${toLabel}\nValue: ${valueLabel}`;
      }
    }

    for (const label of root.querySelectorAll(histogramAxisTimestampSelector)) {
      label.textContent = formatTimelineTickLabelNs(label.dataset.timestampNs, label.dataset.spanWidthNs);
    }
  }

  function formatTimestampNs(nsValue, now) {
    const timestamp = dateFromNs(nsValue);
    if (!timestamp) {
      return "-";
    }

    if (sameLocalDate(timestamp, now)) {
      return formatTime(timestamp, true);
    }

    const timestampWeek = localISOWeek(timestamp);
    const nowWeek = localISOWeek(now);
    if (timestampWeek.year === nowWeek.year && timestampWeek.week === nowWeek.week) {
      return `${weekdayNames[timestamp.getDay()]} ${formatTime(timestamp, true)}`;
    }

    if (timestamp.getFullYear() === now.getFullYear()) {
      return `${pad2(timestamp.getDate())}.${pad2(timestamp.getMonth() + 1)} ${formatTime(timestamp, true)}`;
    }

    return `${pad2(timestamp.getDate())}.${pad2(timestamp.getMonth() + 1)}.${timestamp.getFullYear()} ${formatTime(timestamp, true)}`;
  }

  function formatTimelineTickLabelNs(nsValue, spanWidthNsValue) {
    const timestamp = dateFromNs(nsValue);
    const spanWidthNs = parseBigIntValue(spanWidthNsValue);
    if (!timestamp || spanWidthNs === null) {
      return "-";
    }

    switch (true) {
      case spanWidthNs <= oneDayNs:
        return formatTime(timestamp, false);
      case spanWidthNs <= 7n * oneDayNs:
        return `${pad2(timestamp.getDate())} ${monthNames[timestamp.getMonth()]} ${formatTime(timestamp, false)}`;
      case spanWidthNs <= 90n * oneDayNs:
        return `${pad2(timestamp.getDate())} ${monthNames[timestamp.getMonth()]}`;
      default:
        return `${timestamp.getFullYear()}-${pad2(timestamp.getMonth() + 1)}-${pad2(timestamp.getDate())}`;
    }
  }

  function dateFromNs(nsValue) {
    const ns = parseBigIntValue(nsValue);
    if (ns === null || ns === 0n) {
      return null;
    }
    return new Date(Number(ns / nanosecondsPerMillisecond));
  }

  function sameLocalDate(leftDate, rightDate) {
    return leftDate.getFullYear() === rightDate.getFullYear() &&
      leftDate.getMonth() === rightDate.getMonth() &&
      leftDate.getDate() === rightDate.getDate();
  }

  function localISOWeek(date) {
    const localDateUTC = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
    const day = localDateUTC.getUTCDay() || 7;
    localDateUTC.setUTCDate(localDateUTC.getUTCDate() + 4 - day);
    const yearStart = new Date(Date.UTC(localDateUTC.getUTCFullYear(), 0, 1));
    return {
      week: Math.ceil((((localDateUTC - yearStart) / millisecondsPerDay) + 1) / 7),
      year: localDateUTC.getUTCFullYear()
    };
  }

  function formatTime(date, includeSeconds) {
    const parts = [pad2(date.getHours()), pad2(date.getMinutes())];
    if (includeSeconds) {
      parts.push(pad2(date.getSeconds()));
    }
    return parts.join(":");
  }

  function pad2(value) {
    return String(value).padStart(2, "0");
  }

  function bindForm(form) {
    const searchInput = form.querySelector(`[data-behavior="${searchBehavior}"]`);
    const nodeLimitSelect = form.querySelector("#node-limit");
    const sortInput = form.querySelector(`input[name="sort"]`);

    form.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      if (target.getAttribute("name") === presetBehavior) {
        applyPreset(form, target.value);
        submitForm(form);
        return;
      }

      if (target.getAttribute("name") === "metric") {
        if (sortInput) {
          sortInput.value = defaultSortForMetric(target.value);
        }
        if (target.value === metricDNSLookupsValue) {
          resetDirection(form);
        }
      }

      if (target.getAttribute("name") === granularityName && nodeLimitSelect) {
        nodeLimitSelect.value = nodeLimitAutoValue;
      }

      submitForm(form);
    });

    if (searchInput) {
      searchInput.addEventListener("input", () => {
        window.clearTimeout(searchTimeoutId);
        searchTimeoutId = window.setTimeout(() => submitForm(form), 250);
      });
    }

    const presetValue = selectedPresetValue(form);
    if (presetValue) {
      applyPreset(form, presetValue);
    }
  }

  function bindHistogram(histogram) {
    const svg = histogram.querySelector("svg");
    const form = document.querySelector("#filters-form");
    const bars = Array.from(histogram.querySelectorAll(histogramBarSelector));
    if (!svg || !form || bars.length === 0) {
      return;
    }

    let dragStartIndex = null;
    let overlay = null;

    function indexFromClientX(clientX) {
      const bounds = svg.getBoundingClientRect();
      const relative = Math.max(0, Math.min(bounds.width, clientX - bounds.left));
      return Math.min(bars.length - 1, Math.floor((relative / bounds.width) * bars.length));
    }

    function clearOverlay() {
      dragStartIndex = null;
      hideTooltip();
      if (overlay) {
        overlay.remove();
        overlay = null;
      }
    }

    function showTooltip(bar, clientX, clientY) {
      const tooltip = histogramTooltip();
      const value = document.createElement("strong");
      const range = document.createElement("span");
      value.textContent = `Value: ${bar.dataset.valueLabel || ""}`;
      range.textContent = `${bar.dataset.fromLabel || ""} - ${bar.dataset.toLabel || ""}`;
      tooltip.replaceChildren(value, range);
      positionHistogramTooltip(tooltip, clientX, clientY);
    }

    function hideTooltip() {
      const tooltip = document.querySelector(`.${histogramTooltipClassName}`);
      if (tooltip) {
        tooltip.remove();
      }
    }

    function showTooltipForFocusedBar(bar) {
      const bounds = bar.getBoundingClientRect();
      showTooltip(bar, bounds.left + bounds.width / 2, bounds.top);
    }

    for (const bar of bars) {
      bar.addEventListener("mouseenter", (event) => showTooltip(bar, event.clientX, event.clientY));
      bar.addEventListener("mousemove", (event) => {
        if (dragStartIndex !== null) {
          return;
        }
        showTooltip(bar, event.clientX, event.clientY);
      });
      bar.addEventListener("mouseleave", hideTooltip);
      bar.addEventListener("focus", () => showTooltipForFocusedBar(bar));
      bar.addEventListener("blur", hideTooltip);
    }

    svg.addEventListener("mousedown", (event) => {
      hideTooltip();
      dragStartIndex = indexFromClientX(event.clientX);
      overlay = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      overlay.setAttribute("y", "0");
      overlay.setAttribute("height", "100%");
      overlay.setAttribute("fill", "rgba(88, 126, 163, 0.18)");
      svg.appendChild(overlay);
    });

    svg.addEventListener("mousemove", (event) => {
      if (dragStartIndex === null || !overlay) {
        return;
      }

      const currentIndex = indexFromClientX(event.clientX);
      const barWidth = svg.viewBox.baseVal.width / bars.length;
      const left = Math.min(dragStartIndex, currentIndex) * barWidth;
      const right = (Math.max(dragStartIndex, currentIndex) + 1) * barWidth;
      overlay.setAttribute("x", `${left}`);
      overlay.setAttribute("width", `${Math.max(3, right - left)}`);
    });

    svg.addEventListener("mouseleave", clearOverlay);

    svg.addEventListener("mouseup", (event) => {
      if (dragStartIndex === null) {
        return;
      }

      const currentIndex = indexFromClientX(event.clientX);
      const startIndex = Math.min(dragStartIndex, currentIndex);
      const endIndex = Math.max(dragStartIndex, currentIndex);
      setRange(form, bars[startIndex].dataset.fromNs, bars[endIndex].dataset.toNs);
      clearOverlay();
      submitForm(form);
    });

    svg.addEventListener("dblclick", () => {
      resetRange(form);
      submitForm(form);
    });
  }

  function histogramTooltip() {
    const existingTooltip = document.querySelector(`.${histogramTooltipClassName}`);
    if (existingTooltip instanceof HTMLElement) {
      return existingTooltip;
    }

    const tooltip = document.createElement("div");
    tooltip.className = histogramTooltipClassName;
    tooltip.setAttribute("role", "tooltip");
    document.body.appendChild(tooltip);
    return tooltip;
  }

  function positionHistogramTooltip(tooltip, clientX, clientY) {
    tooltip.style.left = "0";
    tooltip.style.top = "0";
    const bounds = tooltip.getBoundingClientRect();
    const maxLeft = window.innerWidth - bounds.width - histogramTooltipOffsetPx;
    const maxTop = window.innerHeight - bounds.height - histogramTooltipOffsetPx;
    const preferredLeft = clientX + histogramTooltipOffsetPx;
    const preferredTop = clientY + histogramTooltipOffsetPx;
    tooltip.style.left = `${Math.max(histogramTooltipOffsetPx, Math.min(maxLeft, preferredLeft))}px`;
    tooltip.style.top = `${Math.max(histogramTooltipOffsetPx, Math.min(maxTop, preferredTop))}px`;
  }

  function hideHistogramTooltip() {
    const tooltip = document.querySelector(`.${histogramTooltipClassName}`);
    if (tooltip) {
      tooltip.remove();
    }
  }

  function applyPreset(form, presetValue) {
    const appShell = document.querySelector("#app-shell");
    if (!appShell) {
      return;
    }

    const spanStartNs = parseBigIntValue(appShell.dataset.spanStartNs);
    const spanEndNs = parseBigIntValue(appShell.dataset.spanEndNs);
    if (spanStartNs === null || spanEndNs === null) {
      return;
    }

    let fromNs = spanStartNs;
    let toNs = spanEndNs;

    switch (presetValue) {
      case presetHourValue:
        fromNs = maxBigInt(spanStartNs, spanEndNs - oneHourNs);
        break;
      case presetDayValue:
      case presetDayLegacyValue:
        fromNs = maxBigInt(spanStartNs, spanEndNs - oneDayNs);
        break;
      case presetWeekValue:
        fromNs = maxBigInt(spanStartNs, spanEndNs - 7n * oneDayNs);
        break;
      case presetMonthValue:
        fromNs = maxBigInt(spanStartNs, spanEndNs - 30n * oneDayNs);
        break;
      default:
        break;
    }

    setRange(form, fromNs.toString(), toNs.toString());
  }

  function setRange(form, fromNs, toNs) {
    const fromInput = form.querySelector("#filter-from-ns");
    const toInput = form.querySelector("#filter-to-ns");
    if (fromInput) {
      fromInput.value = fromNs;
    }
    if (toInput) {
      toInput.value = toNs;
    }
  }

  function resetRange(form) {
    const appShell = document.querySelector("#app-shell");
    if (!appShell) {
      return;
    }

    setRange(form, appShell.dataset.spanStartNs || "", appShell.dataset.spanEndNs || "");

    const allPresetInput = form.querySelector(`input[name="${presetBehavior}"][value="${presetAllValue}"]`);
    if (allPresetInput instanceof HTMLInputElement) {
      allPresetInput.checked = true;
    }
  }

  function submitForm(form) {
    if (!entityActionsEnabled(form)) {
      clearEntityActionFields(form);
    }
    form.requestSubmit();
  }

  function entityActionsEnabled(form) {
    const fromInput = form.querySelector("#filter-from-ns");
    const toInput = form.querySelector("#filter-to-ns");
    const fromNs = fromInput instanceof HTMLInputElement ? parseBigIntValue(fromInput.value) : null;
    const toNs = toInput instanceof HTMLInputElement ? parseBigIntValue(toInput.value) : null;
    if (fromNs === null || toNs === null || toNs < fromNs) {
      return true;
    }
    return toNs - fromNs <= entityActionMaxRangeNs;
  }

  function clearEntityActionFields(form) {
    for (const name of ["selected_entity", "selected_edge_src", "selected_edge_dst"]) {
      const input = form.querySelector(`input[name="${name}"]`);
      if (input instanceof HTMLInputElement) {
        input.value = "";
      }
    }

    for (const input of form.querySelectorAll(`input[name="include"], input[name="exclude"]`)) {
      input.remove();
    }
  }

  function resetDirection(form) {
    const bothInput = form.querySelector(`input[name="${directionName}"][value="${directionBothValue}"]`);
    if (bothInput instanceof HTMLInputElement) {
      bothInput.checked = true;
    }
  }

  function bindGraph(graphCanvas) {
    const svg = graphCanvas.querySelector("svg");
    const scene = svg ? svg.querySelector(graphSceneSelector) : null;
    if (!svg || !scene) {
      return;
    }

    let isDragging = false;
    let lastClientX = 0;
    let lastClientY = 0;
    let scale = 1;
    let translateX = 0;
    let translateY = 0;
    let labelFramePending = false;
    let cachedLabels = cacheGraphLabels(scene);

    function interactiveGraphTarget(target) {
      if (!(target instanceof Element)) {
        return null;
      }
      return target.closest("a");
    }

    function updateTransform() {
      scene.setAttribute("transform", `matrix(${scale} 0 0 ${scale} ${translateX} ${translateY})`);
      scheduleLabelVisibilityUpdate();
    }

    function resetView() {
      isDragging = false;
      scale = 1;
      translateX = 0;
      translateY = 0;
      graphCanvas.classList.remove("is-panning");
      updateTransform();
    }

    function scheduleLabelVisibilityUpdate() {
      if (labelFramePending) {
        return;
      }
      labelFramePending = true;
      window.requestAnimationFrame(() => {
        labelFramePending = false;
        updateLabelVisibility(svg, graphCanvas, cachedLabels, scale, translateX, translateY);
      });
    }

    function clientToSVGPoint(clientX, clientY) {
      if (typeof svg.createSVGPoint === "function") {
        const point = svg.createSVGPoint();
        point.x = clientX;
        point.y = clientY;
        const screenMatrix = svg.getScreenCTM();
        if (screenMatrix) {
          return point.matrixTransform(screenMatrix.inverse());
        }
      }

      const bounds = svg.getBoundingClientRect();
      const viewBoxWidth = svg.viewBox.baseVal.width || bounds.width;
      const viewBoxHeight = svg.viewBox.baseVal.height || bounds.height;
      return {
        x: ((clientX - bounds.left) / bounds.width) * viewBoxWidth,
        y: ((clientY - bounds.top) / bounds.height) * viewBoxHeight
      };
    }

    function pointerToScenePoint(clientX, clientY) {
      const point = clientToSVGPoint(clientX, clientY);
      return {
        x: (point.x - translateX) / scale,
        y: (point.y - translateY) / scale
      };
    }

    svg.addEventListener("wheel", (event) => {
      event.preventDefault();
      const point = clientToSVGPoint(event.clientX, event.clientY);
      const scenePoint = pointerToScenePoint(event.clientX, event.clientY);
      const zoomFactor = event.deltaY < 0 ? 1.12 : 0.9;
      const nextScale = Math.max(minGraphScale, Math.min(maxGraphScale, scale * zoomFactor));
      if (nextScale === scale) {
        return;
      }
      scale = nextScale;
      translateX = point.x - scenePoint.x * scale;
      translateY = point.y - scenePoint.y * scale;
      updateTransform();
    }, { passive: false });

    svg.addEventListener("pointerdown", (event) => {
      if (event.button !== 0) {
        return;
      }
      if (interactiveGraphTarget(event.target)) {
        return;
      }
      isDragging = true;
      lastClientX = event.clientX;
      lastClientY = event.clientY;
      svg.setPointerCapture(event.pointerId);
      graphCanvas.classList.add("is-panning");
    });

    svg.addEventListener("pointermove", (event) => {
      if (!isDragging) {
        return;
      }
      const previousPoint = clientToSVGPoint(lastClientX, lastClientY);
      const currentPoint = clientToSVGPoint(event.clientX, event.clientY);
      translateX += currentPoint.x - previousPoint.x;
      translateY += currentPoint.y - previousPoint.y;
      lastClientX = event.clientX;
      lastClientY = event.clientY;
      updateTransform();
    });

    function stopDragging(event) {
      if (!isDragging) {
        return;
      }
      isDragging = false;
      if (typeof event.pointerId === "number") {
        svg.releasePointerCapture(event.pointerId);
      }
      graphCanvas.classList.remove("is-panning");
    }

    svg.addEventListener("pointerup", stopDragging);
    svg.addEventListener("pointercancel", stopDragging);

    svg.addEventListener("dblclick", (event) => {
      event.preventDefault();
      resetView();
    });

    svg.addEventListener("mouseenter", () => {
      cachedLabels = cacheGraphLabels(scene);
      scheduleLabelVisibilityUpdate();
    });

    updateTransform();
  }

  function bindSectionToggles(root) {
    const toggles = root.querySelectorAll(collapsibleToggleSelector);
    for (const toggle of toggles) {
      if (!(toggle instanceof HTMLButtonElement) || toggle.dataset.initialized) {
        continue;
      }
      toggle.dataset.initialized = "true";
      applySectionToggleState(toggle, toggle.getAttribute("aria-expanded") === "true");
      toggle.addEventListener("click", () => {
        const expanded = toggle.getAttribute("aria-expanded") !== "true";
        applySectionToggleState(toggle, expanded);
      });
    }
  }

  function applySectionToggleState(toggle, expanded) {
    const contentID = toggle.getAttribute("aria-controls");
    if (!contentID) {
      return;
    }

    const content = document.getElementById(contentID);
    if (!content) {
      return;
    }

    toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
    toggle.setAttribute("aria-label", sectionToggleAriaLabel(toggle, expanded));

    content.classList.toggle(collapsedClassName, !expanded);
  }

  function sectionToggleAriaLabel(toggle, expanded) {
    const sectionTitle = toggle.dataset.sectionTitle || "section";
    return `${expanded ? "Collapse" : "Expand"} ${sectionTitle}`;
  }

  function cacheGraphLabels(scene) {
    const nodeGroups = Array.from(scene.querySelectorAll(".graph-node"));
    const cachedLabels = [];
    for (const nodeGroup of nodeGroups) {
      const label = nodeGroup.querySelector(".graph-label");
      if (!(label instanceof SVGGraphicsElement)) {
        continue;
      }

      const labelBounds = label.getBBox();
      const translate = parseTranslate(nodeGroup.getAttribute("transform"));
      cachedLabels.push({
        box: {
          bottom: translate.y + labelBounds.y + labelBounds.height,
          left: translate.x + labelBounds.x,
          right: translate.x + labelBounds.x + labelBounds.width,
          top: translate.y + labelBounds.y
        },
        nodeGroup,
        persistent: nodeGroup.dataset.labelPersistent === "true",
        priority: Number(nodeGroup.dataset.nodePriority || "0")
      });
    }

    cachedLabels.sort((leftLabel, rightLabel) => {
      if (leftLabel.persistent !== rightLabel.persistent) {
        return leftLabel.persistent ? -1 : 1;
      }
      return rightLabel.priority - leftLabel.priority;
    });
    return cachedLabels;
  }

  function updateLabelVisibility(svg, graphCanvas, cachedLabels, scale, translateX, translateY) {
    if (cachedLabels.length === 0) {
      return;
    }

    const viewBox = svg.viewBox.baseVal;
    const canvasBounds = {
      bottom: viewBox.height,
      left: 0,
      right: viewBox.width,
      top: 0
    };
    const placedBoxes = [];

    for (const { box, nodeGroup, persistent } of cachedLabels) {
      const labelBox = transformSceneBox(box, scale, translateX, translateY);
      if (persistent) {
        nodeGroup.dataset.labelVisible = "true";
        placedBoxes.push(expandRect(labelBox, sceneLabelPaddingPx));
        continue;
      }

      nodeGroup.dataset.labelVisible = "true";
      const paddedBox = expandRect(labelBox, sceneLabelPaddingPx);
      if (!rectOverlaps(paddedBox, canvasBounds) || placedBoxes.some((placedBox) => rectOverlaps(paddedBox, placedBox))) {
        nodeGroup.dataset.labelVisible = "false";
        continue;
      }
      placedBoxes.push(paddedBox);
    }
  }

  function transformSceneBox(box, scale, translateX, translateY) {
    return {
      bottom: box.bottom * scale + translateY,
      left: box.left * scale + translateX,
      right: box.right * scale + translateX,
      top: box.top * scale + translateY
    };
  }

  function expandRect(rect, paddingPx) {
    return {
      bottom: rect.bottom + paddingPx,
      left: rect.left - paddingPx,
      right: rect.right + paddingPx,
      top: rect.top - paddingPx
    };
  }

  function parseTranslate(transformValue) {
    const match = /^translate\(([-0-9.]+),\s*([-0-9.]+)\)$/.exec(transformValue || "");
    if (!match) {
      return { x: 0, y: 0 };
    }
    return {
      x: Number(match[1]),
      y: Number(match[2])
    };
  }

  function rectOverlaps(leftRect, rightRect) {
    return leftRect.left < rightRect.right &&
      leftRect.right > rightRect.left &&
      leftRect.top < rightRect.bottom &&
      leftRect.bottom > rightRect.top;
  }

  function defaultSortForMetric(metricValue) {
    if (metricValue === "connections" || metricValue === metricDNSLookupsValue) {
      return metricValue;
    }
    return "bytes";
  }

  function maxBigInt(leftValue, rightValue) {
    return leftValue > rightValue ? leftValue : rightValue;
  }

  function parseBigIntValue(value) {
    if (!value) {
      return null;
    }
    try {
      return BigInt(value);
    } catch (_error) {
      return null;
    }
  }

  function selectedPresetValue(form) {
    const checkedInput = form.querySelector(`input[name="${presetBehavior}"]:checked`);
    if (!(checkedInput instanceof HTMLInputElement)) {
      return "";
    }
    return checkedInput.value;
  }

  function statusElement() {
    return document.querySelector(statusSelector);
  }

  function setLoadingStatus(message) {
    const element = statusElement();
    if (!element) {
      return;
    }

    element.textContent = message;
    element.classList.add(loadingClassName);
    element.classList.remove(errorClassName);
  }

  function setErrorStatus(message) {
    const element = statusElement();
    if (!element) {
      return;
    }

    element.textContent = message;
    element.classList.add(errorClassName);
    element.classList.remove(loadingClassName);
  }

  function clearStatus() {
    const element = statusElement();
    if (!element) {
      return;
    }

    element.textContent = "";
    element.classList.remove(loadingClassName, errorClassName);
  }

  function initializeHotReload() {
    if (hotReloadInitialized) {
      return;
    }

    const body = document.body;
    if (!body || body.dataset.devMode !== "true") {
      return;
    }

    const startupSessionToken = body.dataset.devSessionToken || "";
    if (!startupSessionToken) {
      return;
    }

    hotReloadInitialized = true;

    window.setInterval(async () => {
      try {
        const response = await window.fetch(versionPath, { cache: "no-store" });
        if (!response.ok) {
          return;
        }
        const currentSessionToken = (await response.text()).trim();
        if (currentSessionToken && currentSessionToken !== startupSessionToken) {
          window.location.reload();
        }
      } catch (_error) {
      }
    }, reloadCheckIntervalMs);
  }

  document.addEventListener("DOMContentLoaded", () => {
    initialize(document);
    initializeHotReload();
  });
  document.body.addEventListener("htmx:beforeRequest", () => setLoadingStatus(loadingMessage));
  document.body.addEventListener("htmx:beforeSwap", hideHistogramTooltip);
  document.body.addEventListener("htmx:afterSwap", (event) => {
    if (event.target instanceof HTMLElement) {
      initialize(event.target);
    }
    clearStatus();
  });
  document.body.addEventListener("htmx:responseError", (event) => {
    const detail = event.detail;
    if (detail && detail.xhr) {
      setErrorStatus(detail.xhr.responseText || statusErrorMessage);
      return;
    }
    setErrorStatus(statusErrorMessage);
  });
})();
