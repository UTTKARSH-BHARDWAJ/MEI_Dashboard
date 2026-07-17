import re

with open('index.html', 'r') as f:
    html = f.read()

# 1. Variables
html = html.replace(
    "let filtered = [], selected = null, selectedReason = null, selectedMessage = null, selectedDay = null;",
    "let filtered = [], selectedMessage = null;\n        let selectedMachines = [], selectedReasons = [], selectedDays = [];\n\n        function toggleSelection(arr, item) {\n            const idx = arr.indexOf(item);\n            if (idx > -1) arr.splice(idx, 1);\n            else arr.push(item);\n        }"
)

html = html.replace(
    "selected = null; selectedReason = null; selectedMessage = null; selectedDay = null;",
    "selectedMachines = []; selectedReasons = []; selectedMessage = null; selectedDays = [];"
)

# 2. Render initial filters
html = html.replace(
    "const dayFiltered = filtered.filter(d => !selectedDay || d._day === selectedDay);",
    "const dayFiltered = filtered.filter(d => selectedDays.length === 0 || selectedDays.includes(d._day));"
)
html = html.replace(
    "const reasonsData = dayFiltered.filter(d => !selectedReason || d['Reject Detail'] === selectedReason);",
    "const reasonsData = dayFiltered.filter(d => selectedReasons.length === 0 || selectedReasons.includes(d['Reject Detail']));"
)
html = html.replace(
    "const sub = selected ? reasonsData.filter(d => d['Machine Name'] === selected) : reasonsData;",
    "const sub = selectedMachines.length > 0 ? reasonsData.filter(d => selectedMachines.includes(d['Machine Name'])) : reasonsData;"
)

# 3. Titles
html = html.replace(
    "document.getElementById('ranking-title').innerText = selectedReason ? ` Ranking: ${selectedReason}` : ' Racer Ranking';",
    "document.getElementById('ranking-title').innerText = selectedReasons.length ? ` Ranking: ${selectedReasons.length > 1 ? selectedReasons.length + ' selected' : selectedReasons[0]}` : ' Racer Ranking';"
)
html = html.replace(
    "document.getElementById('chart-title').innerText = selected ? ` ${selected} Reasons` : ' Root Cause Breakdown';",
    "document.getElementById('chart-title').innerText = selectedMachines.length ? ` ${selectedMachines.length > 1 ? selectedMachines.length + ' Machines' : selectedMachines[0]} Reasons` : ' Root Cause Breakdown';"
)
html = html.replace(
    "document.getElementById('scroll-title').innerText = selected ? ` ${selected} Rejection Reasons` : ' Global Rejection Reasons';",
    "document.getElementById('scroll-title').innerText = selectedMachines.length ? ` ${selectedMachines.length > 1 ? selectedMachines.length + ' Machines' : selectedMachines[0]} Rejection Reasons` : ' Global Rejection Reasons';"
)
html = html.replace(
    "document.getElementById('daily-title').innerHTML = (selectedDay ? `📅 Daily Rejection Trend — 📌 ${selectedDay} selected` : '📅 Daily Rejection Trend') +",
    "document.getElementById('daily-title').innerHTML = (selectedDays.length ? `📅 Daily Rejection Trend — 📌 ${selectedDays.length > 1 ? selectedDays.length + ' selected' : selectedDays[0]}` : '📅 Daily Rejection Trend') +"
)

# 4. Leaderboard
html = html.replace(
    "onclick=\"selected=(selected==='${n}'?null:'${n}');render()\"",
    "onclick=\"toggleSelection(selectedMachines, '${n}'); render()\""
)
html = html.replace(
    "${selected===n?'active':''}",
    "${selectedMachines.includes(n)?'active':''}"
)

# 5. machineErrorsChart
html = html.replace(
    "backgroundColor: sorted.map(x => (selected === x[0] ? '#e17055' : (selected ? '#b2bec3' : '#0984e3')))",
    "backgroundColor: sorted.map(x => (selectedMachines.length === 0 ? '#0984e3' : (selectedMachines.includes(x[0]) ? '#e17055' : '#b2bec3')))"
)
html = html.replace(
    "selected = (selected === label ? null : label);",
    "toggleSelection(selectedMachines, label);"
)

# 6. scrollChart
html = html.replace(
    "const globalFiltered = selected ? dayFiltered.filter(d => d['Machine Name'] === selected) : dayFiltered;",
    "const globalFiltered = selectedMachines.length > 0 ? dayFiltered.filter(d => selectedMachines.includes(d['Machine Name'])) : dayFiltered;"
)
html = html.replace(
    "backgroundColor: sortedGlobal.map(x => (selectedReason === x[0] ? '#d63031' : (selectedReason ? '#b2bec3' : '#e17055')))",
    "backgroundColor: sortedGlobal.map(x => (selectedReasons.length === 0 ? '#e17055' : (selectedReasons.includes(x[0]) ? '#d63031' : '#b2bec3')))"
)
html = html.replace(
    "selectedReason = (selectedReason === label ? null : label);",
    "toggleSelection(selectedReasons, label);"
)

# 7. dailyChart
html = html.replace(
    "(!selectedReason || d['Reject Detail'] === selectedReason) &&",
    "(selectedReasons.length === 0 || selectedReasons.includes(d['Reject Detail'])) &&"
)
html = html.replace(
    "(!selected || d['Machine Name'] === selected)",
    "(selectedMachines.length === 0 || selectedMachines.includes(d['Machine Name']))"
)
html = html.replace(
    "backgroundColor: dayLabels.map(d => (selectedDay === d ? '#d63031' : (selectedDay ? '#b2bec3' : '#0984e3')))",
    "backgroundColor: dayLabels.map(d => (selectedDays.length === 0 ? '#0984e3' : (selectedDays.includes(d) ? '#d63031' : '#b2bec3')))"
)
html = html.replace(
    "selectedDay = (selectedDay === day ? null : day);",
    "toggleSelection(selectedDays, day);"
)

# 8. hourlyChart & pieChart
html = html.replace(
    "if (!selected || d['Machine Name'] === selected) hourCounts[d.Hour]++;",
    "if (selectedMachines.length === 0 || selectedMachines.includes(d['Machine Name'])) hourCounts[d.Hour]++;"
)

# 9. showLogs logic
html = html.replace(
    "const base = filtered.filter(d => !selectedReason || d['Reject Detail'] === selectedReason);",
    "const base = filtered.filter(d => selectedReasons.length === 0 || selectedReasons.includes(d['Reject Detail']));"
)
html = html.replace(
    "const sub = selected ? base.filter(d => d['Machine Name'] === selected) : base;",
    "const sub = selectedMachines.length > 0 ? base.filter(d => selectedMachines.includes(d['Machine Name'])) : base;"
)

# 10. openTopMachineModal
html = html.replace(
    "const dayFiltered = filtered.filter(d => !selectedDay || d._day === selectedDay);",
    "const dayFiltered = filtered.filter(d => selectedDays.length === 0 || selectedDays.includes(d._day));"
)


with open('index.html', 'w') as f:
    f.write(html)
