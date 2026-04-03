
            document.addEventListener("DOMContentLoaded", () => {
                const sheetUrlInput = document.getElementById("sheet-url-input");
                const sheetNameInput = document.getElementById("sheet-name-input");
                const setSheetForm = document.querySelector("form[action='/set-sheet']");
                const setColumnsForm = document.getElementById("set-columns-form");
                const sheetTabsState = document.getElementById("sheet-tabs-state");
                const sheetTabsList = document.getElementById("sheet-tabs-list");
                const sheetNameOptions = document.getElementById("sheet-name-options");
                const scheduleForm = document.querySelector("form[action='/set-schedule']");
                const scheduleModeSelect = document.getElementById("schedule-mode-select");
                const weekdaySelect = document.getElementById("schedule-weekday-select");
                const monthDateInput = document.getElementById("schedule-monthdate-input");
                const monthDayInput = document.querySelector("input[name='monthday']");
                const endDateInput = document.getElementById("schedule-enddate-input");
                const monthDateBtn = document.getElementById("monthdate-picker-btn");
                const endDateBtn = document.getElementById("enddate-picker-btn");
                const scheduleMonthdateHelp = document.getElementById("schedule-monthdate-help");
                const scheduleBoundSheetName = document.getElementById("schedule-bound-sheet-name");
                const scheduleBoundSheetId = document.getElementById("schedule-bound-sheet-id");
                const scheduleBoundScope = document.getElementById("schedule-bound-scope");
                const scheduleBoundLink = document.getElementById("schedule-bound-link");
                const scheduleTrackNext = document.getElementById("schedule-track-next");
                const scheduleTrackStarted = document.getElementById("schedule-track-started");
                const scheduleTrackFinished = document.getElementById("schedule-track-finished");
                const scheduleTrackDuration = document.getElementById("schedule-track-duration");
                const scheduleTrackRunning = document.getElementById("schedule-track-running");
                const scheduleTrackStatus = document.getElementById("schedule-track-status");
                const scheduleTrackSource = document.getElementById("schedule-track-source");
                const scheduleTrackSheet = document.getElementById("schedule-track-sheet");
                const scheduleTrackProcessed = document.getElementById("schedule-track-processed");
                const scheduleTrackSuccess = document.getElementById("schedule-track-success");
                const scheduleTrackFailed = document.getElementById("schedule-track-failed");
                const scheduleTrackHistory = document.getElementById("schedule-track-history");
                const themeToggle = document.getElementById("theme-toggle");
                const themeToggleIcon = document.getElementById("theme-toggle-icon");
                const themeToggleLabel = document.getElementById("theme-toggle-label");
                const themeToggleMeta = document.getElementById("theme-toggle-meta");
                const authPolicyText = document.getElementById("auth-policy-text");
                const saveAccessPolicyBtn = document.getElementById("save-access-policy-btn");
                const mailSmtpHost = document.getElementById("mail-smtp-host");
                const mailSmtpPort = document.getElementById("mail-smtp-port");
                const mailSmtpUser = document.getElementById("mail-smtp-user");
                const mailSmtpPassword = document.getElementById("mail-smtp-password");
                const mailFromEmail = document.getElementById("mail-from-email");
                const mailFromName = document.getElementById("mail-from-name");
                const mailUseTls = document.getElementById("mail-use-tls");
                const mailUseSsl = document.getElementById("mail-use-ssl");
                const saveMailConfigBtn = document.getElementById("save-mail-config-btn");
                const employeeUsersData = document.getElementById("employee-users-data");
                const employeeSearchInput = document.getElementById("employee-search-input");
                const employeeRoleFilter = document.getElementById("employee-role-filter");
                const employeeStatusChips = Array.from(document.querySelectorAll(".employee-status-chip"));
                const employeeTableBody = document.getElementById("employee-table-body");
                const employeeEmptyPanel = document.getElementById("employee-empty-panel");
                const employeeEmailInput = document.getElementById("employee-email-input");
                const employeeRoleInput = document.getElementById("employee-role-input");
                const employeeAddBtn = document.getElementById("employee-add-btn");
                const employeeSaveBtn = document.getElementById("employee-save-btn");
                const employeeImportBtn = document.getElementById("employee-import-btn");
                const employeeImportInput = document.getElementById("employee-import-input");
                const employeeResetBtn = document.getElementById("employee-reset-btn");
                const employeeTotalCount = document.getElementById("employee-total-count");
                const employeeVerifiedCount = document.getElementById("employee-verified-count");
                const employeeAdminCount = document.getElementById("employee-admin-count");
                const employeeChipAll = document.getElementById("employee-chip-all");
                const employeeChipPending = document.getElementById("employee-chip-pending");
                const employeeChipVerified = document.getElementById("employee-chip-verified");
                const noticeBanner = document.getElementById("notice-banner");
                const activeSheetNameEls = Array.from(document.querySelectorAll("[data-active-sheet-name]"));
                const activeSheetIdEls = Array.from(document.querySelectorAll("[data-active-sheet-id]"));
                const configModeEls = Array.from(document.querySelectorAll("[data-config-mode]"));
                const startRowEls = Array.from(document.querySelectorAll("[data-start-row-display]"));
                const columnDetectedTextEls = Array.from(document.querySelectorAll("[data-column-detected-text]"));
                const metricColEls = Object.fromEntries(
                    ["link", "campaign", "view", "like", "share", "comment", "save"].map((field) => [
                        field,
                        Array.from(document.querySelectorAll(`[data-metric-col="${field}"]`)),
                    ])
                );
                let monthPicker = null;
                let endPicker = null;
                let sheetTabsRequestId = 0;
                let sheetTabsDebounce = null;
                let employeeUsersState = [];
                let employeeStatusFilter = "all";

                const applyTheme = (theme) => {
                    const normalizedTheme = theme === "light" ? "light" : "dark";
                    document.documentElement.dataset.theme = normalizedTheme;
                    if (themeToggleIcon) {
                        themeToggleIcon.innerHTML = normalizedTheme === "light"
                            ? '<i class="fa-solid fa-sun"></i>'
                            : '<i class="fa-solid fa-moon"></i>';
                    }
                    if (themeToggleLabel) {
                        themeToggleLabel.textContent = normalizedTheme === "light" ? "Sáng" : "Tối";
                    }
                    if (themeToggleMeta) {
                        themeToggleMeta.textContent = normalizedTheme === "light"
                            ? "Nhấn để đổi sang tối"
                            : "Nhấn để đổi sang sáng";
                    }
                    if (themeToggle) {
                        const nextThemeText = normalizedTheme === "light" ? "Đổi sang tối" : "Đổi sang sáng";
                        themeToggle.setAttribute("title", nextThemeText);
                        themeToggle.setAttribute("aria-label", nextThemeText);
                    }
                };

                applyTheme(document.documentElement.dataset.theme || "dark");
                if (themeToggle) {
                    themeToggle.addEventListener("click", () => {
                        const nextTheme = document.documentElement.dataset.theme === "light" ? "dark" : "light";
                        applyTheme(nextTheme);
                        try {
                            localStorage.setItem("dashboard_theme", nextTheme);
                        } catch (_) {
                        }
                    });
                }

                const restoreDraft = (el, key) => {
                    if (!el) return;
                    const saved = sessionStorage.getItem(key);
                    if (saved && (!el.value || el.value.trim() === "")) {
                        el.value = saved;
                    }
                    el.addEventListener("input", () => sessionStorage.setItem(key, el.value));
                };

                restoreDraft(sheetUrlInput, "draft_sheet_url");
                restoreDraft(sheetNameInput, "draft_sheet_name");

                const setSheetTabsMessage = (message = "", tone = "muted") => {
                    if (!sheetTabsState) return;
                    const toneMap = {
                        muted: "text-slate-500",
                        loading: "text-cyan-300",
                        success: "text-emerald-300",
                        error: "text-amber-300",
                    };
                    sheetTabsState.className = `text-xs ${
                        toneMap[tone] || toneMap.muted
                    }`;
                    sheetTabsState.textContent = message;
                };

                const clearSheetTabs = () => {
                    if (sheetNameOptions) {
                        sheetNameOptions.innerHTML = "";
                    }
                    if (sheetTabsList) {
                        sheetTabsList.innerHTML = "";
                        sheetTabsList.classList.add("hidden");
                    }
                };

                const renderSheetTabs = (tabs) => {
                    if (sheetNameOptions) {
                        sheetNameOptions.innerHTML = tabs
                            .map((tab) => `<option value="${tab.title}"></option>`)
                            .join("");
                    }
                    if (!sheetTabsList) return;
                    if (!tabs.length) {
                        sheetTabsList.innerHTML = "";
                        sheetTabsList.classList.add("hidden");
                        return;
                    }
                    const selectedTitle = (sheetNameInput?.value || "").trim();
                    sheetTabsList.innerHTML = tabs
                        .map((tab) => {
                            const activeClass = tab.title === selectedTitle ? " is-active" : "";
                            return `<button type="button" class="sheet-tab-chip${activeClass}" data-sheet-tab="${tab.title}">${tab.title}</button>`;
                        })
                        .join("");
                    sheetTabsList.classList.remove("hidden");
                    sheetTabsList.querySelectorAll("[data-sheet-tab]").forEach((button) => {
                        button.addEventListener("click", () => {
                            if (sheetNameInput) {
                                sheetNameInput.value = button.dataset.sheetTab || "";
                                sessionStorage.setItem("draft_sheet_name", sheetNameInput.value);
                                renderSheetTabs(tabs);
                                sheetNameInput.focus();
                            }
                        });
                    });
                };

                const shouldLookupSheetTabs = (value) => {
                    const trimmed = (value || "").trim();
                    return trimmed.length >= 20 || trimmed.includes("/spreadsheets/");
                };

                const fetchSheetTabs = async (value, silent = false) => {
                    const rawValue = (value || "").trim();
                    if (!rawValue) {
                        clearSheetTabs();
                        setSheetTabsMessage("Dán link Google Sheet để hiện danh sách tab có trong file.");
                        return;
                    }
                    if (!shouldLookupSheetTabs(rawValue)) {
                        clearSheetTabs();
                        setSheetTabsMessage("Tiếp tục nhập link hoặc Sheet ID để tải danh sách tab.");
                        return;
                    }

                    const requestId = ++sheetTabsRequestId;
                    if (!silent) {
                        setSheetTabsMessage("Đang tải danh sách tab...", "loading");
                    }

                    try {
                        const response = await fetch(`/sheet-tabs?sheet_url=${encodeURIComponent(rawValue)}`, {
                            headers: { "X-Requested-With": "fetch" },
                            cache: "no-store",
                        });
                        if (!response.ok) {
                            throw new Error("Không gọi được API danh sách tab.");
                        }
                        const data = await response.json();
                        if (requestId !== sheetTabsRequestId) return;

                        if (!data.ok) {
                            clearSheetTabs();
                            setSheetTabsMessage(data.message || "Không tải được danh sách tab.", "error");
                            return;
                        }

                        renderSheetTabs(Array.isArray(data.tabs) ? data.tabs : []);
                        setSheetTabsMessage(data.message || "Đã tải danh sách tab.", "success");
                    } catch (_) {
                        if (requestId !== sheetTabsRequestId) return;
                        clearSheetTabs();
                        setSheetTabsMessage("Không tải được danh sách tab. Kiểm tra link sheet và quyền truy cập.", "error");
                    }
                };

                const scheduleSheetTabsFetch = () => {
                    if (sheetTabsDebounce) {
                        clearTimeout(sheetTabsDebounce);
                    }
                    sheetTabsDebounce = setTimeout(() => {
                        fetchSheetTabs(sheetUrlInput?.value || "");
                    }, 450);
                };

                if (sheetUrlInput) {
                    sheetUrlInput.addEventListener("input", scheduleSheetTabsFetch);
                    sheetUrlInput.addEventListener("blur", () => fetchSheetTabs(sheetUrlInput.value, true));
                }
                if (sheetNameInput) {
                    sheetNameInput.addEventListener("input", () => {
                        if (sheetTabsList && !sheetTabsList.classList.contains("hidden")) {
                            sheetTabsList.querySelectorAll("[data-sheet-tab]").forEach((button) => {
                                button.classList.toggle("is-active", (button.dataset.sheetTab || "") === sheetNameInput.value.trim());
                            });
                        }
                    });
                }
                if (sheetUrlInput?.value) {
                    fetchSheetTabs(sheetUrlInput.value, true);
                }

                if (setSheetForm) {
                    setSheetForm.addEventListener("submit", async (event) => {
                        event.preventDefault();
                        const params = new URLSearchParams(new FormData(setSheetForm));
                        try {
                            const response = await fetch(`/set-sheet?${params.toString()}`, {
                                headers: { "X-Requested-With": "fetch" },
                                cache: "no-store",
                            });
                            const data = await response.json();
                            if (data.ok) {
                                sessionStorage.removeItem("draft_sheet_url");
                                sessionStorage.removeItem("draft_sheet_name");
                                applyActiveSheetMeta(data, true);
                                applyColumnConfigState(data);
                                applyScheduleConfigState(data);
                                applyScheduleTrackingState(data);
                                if (sheetUrlInput?.value) {
                                    fetchSheetTabs(sheetUrlInput.value, true);
                                }
                            }
                            applyStatusState(data);
                            showNotice(
                                data.message || (data.ok ? "Đã lưu sheet thành công." : "Không lưu được sheet."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        } catch (_) {
                            showNotice("Không lưu được sheet. Vui lòng thử lại.", "error");
                        }
                    });
                }

                if (setColumnsForm) {
                    setColumnsForm.addEventListener("submit", async (event) => {
                        event.preventDefault();
                        const params = new URLSearchParams();
                        document.querySelectorAll("[form='set-columns-form'][name]").forEach((field) => {
                            params.set(field.name, field.value || "");
                        });
                        try {
                            const response = await fetch(`/set-columns?${params.toString()}`, {
                                headers: { "X-Requested-With": "fetch" },
                                cache: "no-store",
                            });
                            const data = await response.json();
                            applyStatusState(data);
                            applyScheduleConfigState(data);
                            applyScheduleTrackingState(data);
                            if (data.ok) {
                                applyColumnConfigState(data);
                            }
                            showNotice(
                                data.message || (data.ok ? "Đã lưu cấu hình nhập liệu thành công." : "Không lưu được cấu hình nhập liệu."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        } catch (_) {
                            showNotice("Không lưu được cấu hình nhập liệu. Vui lòng thử lại.", "error");
                        }
                    });
                }

                if (saveAccessPolicyBtn && authPolicyText) {
                    saveAccessPolicyBtn.addEventListener("click", async () => {
                        try {
                            const response = await fetch("/admin/save-access-policy", {
                                method: "POST",
                                headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
                                body: JSON.stringify({ policy_text: authPolicyText.value || "" }),
                            });
                            const data = await response.json();
                            if (data.ok && typeof data.policy_text === "string") {
                                authPolicyText.value = data.policy_text;
                            }
                            showNotice(
                                data.message || (data.ok ? "Đã lưu access policy." : "Không lưu được access policy."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        } catch (_) {
                            showNotice("Không lưu được access policy. Vui lòng thử lại.", "error");
                        }
                    });
                }

                if (saveMailConfigBtn) {
                    saveMailConfigBtn.addEventListener("click", async () => {
                        const payload = {
                            smtp_host: mailSmtpHost?.value || "",
                            smtp_port: mailSmtpPort?.value || "",
                            smtp_user: mailSmtpUser?.value || "",
                            smtp_password: mailSmtpPassword?.value || "",
                            smtp_from_email: mailFromEmail?.value || "",
                            smtp_from_name: mailFromName?.value || "",
                            use_tls: Boolean(mailUseTls?.checked),
                            use_ssl: Boolean(mailUseSsl?.checked),
                        };
                        try {
                            const response = await fetch("/admin/save-mail-config", {
                                method: "POST",
                                headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
                                body: JSON.stringify(payload),
                            });
                            const data = await response.json();
                            showNotice(
                                data.message || (data.ok ? "Đã lưu cấu hình mail." : "Không lưu được cấu hình mail."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        } catch (_) {
                            showNotice("Không lưu được cấu hình mail. Vui lòng thử lại.", "error");
                        }
                    });
                }

                document.addEventListener("click", async (event) => {
                    const actionLink = event.target.closest("[data-inline-action]");
                    if (!actionLink) return;
                    event.preventDefault();

                    const action = actionLink.dataset.inlineAction || "";
                    const baseUrl = actionLink.getAttribute("href") || (action === "stop" ? "/stop" : "/start");
                    let requestUrl = baseUrl;
                    if (action === "start") {
                        const params = new URLSearchParams();
                        const draftSheetUrl = (sheetUrlInput?.value || "").trim();
                        const draftSheetName = (sheetNameInput?.value || "").trim();
                        if (draftSheetUrl) {
                            params.set("sheet_url", draftSheetUrl);
                        }
                        if (draftSheetName) {
                            params.set("sheet_name", draftSheetName);
                        }
                        if (params.toString()) {
                            requestUrl += `?${params.toString()}`;
                        }
                    }

                    try {
                        const response = await fetch(requestUrl, {
                            headers: { "X-Requested-With": "fetch" },
                            cache: "no-store",
                        });
                        const data = await response.json();
                        applyStatusState(data);
                        applyScheduleConfigState(data);
                        applyScheduleTrackingState(data);
                        if (action === "start" && data.ok) {
                            sessionStorage.removeItem("draft_sheet_url");
                            sessionStorage.removeItem("draft_sheet_name");
                            applyActiveSheetMeta(data, true);
                            applyColumnConfigState(data);
                            if (sheetUrlInput?.value) {
                                fetchSheetTabs(sheetUrlInput.value, true);
                            }
                        }
                        showNotice(
                            data.message || (data.ok ? "Đã cập nhật tác vụ." : "Không thực hiện được tác vụ."),
                            data.level || (data.ok ? "success" : "error")
                        );
                    } catch (_) {
                        showNotice(
                            action === "stop"
                                ? "Không dừng được tác vụ. Vui lòng thử lại."
                                : "Không bắt đầu được tác vụ. Vui lòng thử lại.",
                            "error"
                        );
                    }
                });

                const getWeeklyJsDay = () => ((parseInt(weekdaySelect?.value || "0", 10) + 1) % 7);
                const updateSchedulePreview = () => {
                    const mode = scheduleModeSelect?.value || "off";
                    if (scheduleMonthdateHelp) {
                        scheduleMonthdateHelp.textContent = mode === "weekly"
                            ? "Mở lịch để xem toàn bộ ngày đúng thứ đã chọn được khoanh sẵn."
                            : "Hằng tháng: chọn ngày chạy. Hằng tuần: mở lịch để xem các ngày của thứ đã chọn được khoanh sẵn.";
                    }
                };
                const syncScheduleWeekdayHighlights = () => {
                    if (!monthPicker?.calendarContainer) return;
                    const mode = scheduleModeSelect?.value || "off";
                    const activeMonth = monthPicker.currentMonth;
                    const activeYear = monthPicker.currentYear;
                    const targetWeekday = getWeeklyJsDay();
                    monthPicker.calendarContainer.querySelectorAll(".flatpickr-day").forEach((dayElem) => {
                        dayElem.classList.remove("schedule-weekday-match");
                        if (mode !== "weekly" || !dayElem.dateObj) return;
                        if (dayElem.dateObj.getFullYear() !== activeYear || dayElem.dateObj.getMonth() !== activeMonth) return;
                        if (dayElem.dateObj.getDay() === targetWeekday) {
                            dayElem.classList.add("schedule-weekday-match");
                        }
                    });
                };
                const redrawScheduleCalendar = () => {
                    if (monthPicker && typeof monthPicker.redraw === "function") {
                        monthPicker.redraw();
                    }
                    requestAnimationFrame(syncScheduleWeekdayHighlights);
                    updateSchedulePreview();
                };

                if (monthDateInput && typeof flatpickr === "function") {
                    monthPicker = flatpickr(monthDateInput, {
                        dateFormat: "Y-m-d",
                        altInput: true,
                        altFormat: "d/m/Y",
                        locale: (window.flatpickr && flatpickr.l10ns && flatpickr.l10ns.vn) ? "vn" : "default",
                        disableMobile: true,
                        allowInput: true,
                        onDayCreate: (_, __, fp, dayElem) => {
                            dayElem.classList.remove("schedule-weekday-match");
                            const mode = scheduleModeSelect?.value || "off";
                            if (mode !== "weekly" || !dayElem.dateObj) return;
                            if (dayElem.dateObj.getFullYear() !== fp.currentYear || dayElem.dateObj.getMonth() !== fp.currentMonth) return;
                            if (dayElem.dateObj.getDay() === getWeeklyJsDay()) {
                                dayElem.classList.add("schedule-weekday-match");
                            }
                        },
                        onMonthChange: () => redrawScheduleCalendar(),
                        onYearChange: () => redrawScheduleCalendar(),
                        onOpen: () => redrawScheduleCalendar(),
                        onReady: () => redrawScheduleCalendar(),
                        onValueUpdate: () => redrawScheduleCalendar(),
                    });
                }

                if (endDateInput && typeof flatpickr === "function") {
                    endPicker = flatpickr(endDateInput, {
                        dateFormat: "Y-m-d",
                        altInput: true,
                        altFormat: "d/m/Y",
                        locale: (window.flatpickr && flatpickr.l10ns && flatpickr.l10ns.vn) ? "vn" : "default",
                        disableMobile: true,
                        allowInput: true,
                        onChange: () => updateSchedulePreview(),
                    });
                }

                if (scheduleForm && monthDateInput && monthDayInput) {
                    const syncMonthday = () => {
                        if (!monthDateInput.value) return;
                        const parts = monthDateInput.value.split("-");
                        const day = parseInt(parts[2], 10);
                        if (!Number.isNaN(day)) {
                            monthDayInput.value = Math.max(1, Math.min(28, day));
                        }
                    };
                    monthDateInput.addEventListener("change", syncMonthday);
                    scheduleForm.addEventListener("submit", syncMonthday);
                    syncMonthday();

                    scheduleForm.addEventListener("submit", async (event) => {
                        event.preventDefault();
                        syncMonthday();
                        const params = new URLSearchParams(new FormData(scheduleForm));
                        try {
                            const response = await fetch(`/set-schedule?${params.toString()}`, {
                                headers: { "X-Requested-With": "fetch" },
                                cache: "no-store",
                            });
                            const data = await response.json();
                            applyStatusState(data);
                            applyScheduleConfigState(data);
                            applyScheduleTrackingState(data);
                            showNotice(
                                data.message || (data.ok ? "Đã cập nhật lịch tự động." : "Không lưu được lịch tự động."),
                                data.level || (data.ok ? "success" : "error")
                            );
                        } catch (_) {
                            showNotice("Không lưu được lịch tự động. Vui lòng thử lại.", "error");
                        }
                    });
                }
                if (scheduleModeSelect) {
                    scheduleModeSelect.addEventListener("change", redrawScheduleCalendar);
                }
                if (weekdaySelect) {
                    weekdaySelect.addEventListener("change", redrawScheduleCalendar);
                }
                if (endDateInput) {
                    endDateInput.addEventListener("change", updateSchedulePreview);
                }

                if (monthDateInput && monthDateBtn) {
                    monthDateBtn.addEventListener("click", () => {
                        try {
                            if (monthPicker && typeof monthPicker.open === "function") {
                                monthPicker.open();
                            } else if (typeof monthDateInput.showPicker === "function") {
                                monthDateInput.showPicker();
                            } else {
                                monthDateInput.focus();
                                monthDateInput.click();
                            }
                        } catch (_) {
                            monthDateInput.focus();
                        }
                    });
                }
                if (endDateInput && endDateBtn) {
                    endDateBtn.addEventListener("click", () => {
                        try {
                            if (endPicker && typeof endPicker.open === "function") {
                                endPicker.open();
                            } else if (typeof endDateInput.showPicker === "function") {
                                endDateInput.showPicker();
                            } else {
                                endDateInput.focus();
                                endDateInput.click();
                            }
                        } catch (_) {
                            endDateInput.focus();
                        }
                    });
                }
                updateSchedulePreview();
                redrawScheduleCalendar();

                const statusBadge = document.getElementById("status-badge");
                const currentTaskLabel = document.getElementById("current-task");
                const progressBar = document.getElementById("progress-bar");
                const logSection = document.getElementById("log-section");
                const primaryAction = document.getElementById("primary-action");
                const scheduleLabelEls = Array.from(document.querySelectorAll("[data-schedule-label]"));
                const sidebarStatusText = document.getElementById("sidebar-status-text");
                const sidebarStatusTask = document.getElementById("sidebar-status-task");
                const postsVisibleCount = document.getElementById("posts-visible-count");
                const postsActiveTabLabel = document.getElementById("posts-active-tab-label");
                const postsTabCards = Array.from(document.querySelectorAll("[data-posts-tab-trigger]"));
                const postsTabPanels = Array.from(document.querySelectorAll("[data-posts-tab-panel]"));
                const sidebarLinks = Array.from(document.querySelectorAll("[data-nav-link]"));
                const dashboardSections = Array.from(document.querySelectorAll("[data-dashboard-section]"));
                let refreshInFlight = false;

                const showNotice = (message = "", level = "info") => {
                    if (!noticeBanner) return;
                    noticeBanner.innerHTML = "";
                    if (!message) return;
                    const toneMap = {
                        success: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
                        warning: "bg-amber-500/15 text-amber-300 border-amber-500/30",
                        error: "bg-red-500/15 text-red-300 border-red-500/30",
                        info: "bg-cyan-500/15 text-cyan-200 border-cyan-500/30",
                    };
                    const box = document.createElement("div");
                    box.className = `mb-6 px-4 py-3 rounded-xl border text-sm font-bold ${
                        toneMap[level] || toneMap.info
                    }`;
                    box.textContent = message;
                    noticeBanner.appendChild(box);
                };

                const parseEmployeeUsersData = () => {
                    if (!employeeUsersData) return [];
                    try {
                        const parsed = JSON.parse(employeeUsersData.textContent || "[]");
                        return Array.isArray(parsed) ? parsed : [];
                    } catch (_) {
                        return [];
                    }
                };

                const normalizeEmployeeItem = (item) => {
                    const email = String(item?.email || "").trim().toLowerCase();
                    if (!email || !email.includes("@")) return null;
                    const role = String(item?.role || "user").trim().toLowerCase() === "admin" ? "admin" : "user";
                    const lastLoginText = String(item?.last_login_text || "").trim();
                    const statusKey = String(item?.status_key || (lastLoginText && lastLoginText !== "Chưa có" ? "verified" : "pending")).trim() === "verified" ? "verified" : "pending";
                    const loginCount = Math.max(0, Number.parseInt(String(item?.login_count || "0"), 10) || 0);
                    return {
                        email,
                        role,
                        role_label: role === "admin" ? "Admin" : "User",
                        status_key: statusKey,
                        status_label: statusKey === "verified" ? "Đã xác thực" : "Chờ xác thực",
                        last_login_text: lastLoginText || "Chưa có",
                        login_count: loginCount,
                        is_forced_admin: Boolean(item?.is_forced_admin),
                    };
                };

                const dedupeEmployees = (items) => {
                    const map = new Map();
                    (Array.isArray(items) ? items : []).forEach((item) => {
                        const normalized = normalizeEmployeeItem(item);
                        if (!normalized) return;
                        map.set(normalized.email, normalized);
                    });
                    return Array.from(map.values()).sort((a, b) => {
                        const roleCompare = (a.role === "admin" ? 0 : 1) - (b.role === "admin" ? 0 : 1);
                        if (roleCompare !== 0) return roleCompare;
                        return a.email.localeCompare(b.email);
                    });
                };

                const updateEmployeeSummary = (items) => {
                    const rows = Array.isArray(items) ? items : [];
                    const verified = rows.filter((item) => item.status_key === "verified").length;
                    const admins = rows.filter((item) => item.role === "admin").length;
                    const pending = Math.max(0, rows.length - verified);
                    if (employeeTotalCount) employeeTotalCount.textContent = String(rows.length);
                    if (employeeVerifiedCount) employeeVerifiedCount.textContent = String(verified);
                    if (employeeAdminCount) employeeAdminCount.textContent = String(admins);
                    if (employeeChipAll) employeeChipAll.textContent = String(rows.length);
                    if (employeeChipPending) employeeChipPending.textContent = String(pending);
                    if (employeeChipVerified) employeeChipVerified.textContent = String(verified);
                };

                const renderEmployeeRows = () => {
                    if (!employeeTableBody) return;
                    const searchValue = String(employeeSearchInput?.value || "").trim().toLowerCase();
                    const roleValue = String(employeeRoleFilter?.value || "all").trim().toLowerCase();
                    const rows = employeeUsersState.filter((item) => {
                        const matchesSearch = !searchValue || item.email.toLowerCase().includes(searchValue);
                        const matchesRole = roleValue === "all" || item.role === roleValue;
                        const matchesStatus = employeeStatusFilter === "all" || item.status_key === employeeStatusFilter;
                        return matchesSearch && matchesRole && matchesStatus;
                    });
                    employeeTableBody.innerHTML = rows.map((item) => {
                        const forcedHint = item.is_forced_admin ? '<div class="employee-meta">Admin cứng</div>' : `<div class="employee-meta">${item.role_label}</div>`;
                        return `
                            <tr>
                                <td>
                                    <div class="employee-row-user">
                                        <span class="employee-avatar">${item.email.charAt(0).toUpperCase()}</span>
                                        <div>
                                            <div class="employee-email">${item.email}</div>
                                            ${forcedHint}
                                        </div>
                                    </div>
                                </td>
                                <td>
                                    <select class="employee-role-select" data-employee-role="${item.email}" ${item.is_forced_admin ? "disabled" : ""}>
                                        <option value="user" ${item.role === "user" ? "selected" : ""}>User</option>
                                        <option value="admin" ${item.role === "admin" ? "selected" : ""}>Admin</option>
                                    </select>
                                </td>
                                <td><span class="employee-status-badge ${item.status_key === "verified" ? "is-verified" : "is-pending"}">${item.status_label}</span></td>
                                <td>${item.last_login_text || "Chưa có"}</td>
                                <td class="text-right font-black">${item.login_count || 0}</td>
                                <td>
                                    <div class="employee-table-actions">
                                        <button type="button" class="employee-icon-btn" data-employee-remove="${item.email}" title="Xóa nhân viên" ${item.is_forced_admin ? "disabled" : ""}>
                                            <i class="fa-regular fa-trash-can"></i>
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        `;
                    }).join("");
                    const hasRows = rows.length > 0;
                    if (employeeEmptyPanel) employeeEmptyPanel.classList.toggle("hidden", hasRows);

                    employeeTableBody.querySelectorAll("[data-employee-role]").forEach((select) => {
                        select.addEventListener("change", () => {
                            const email = select.getAttribute("data-employee-role") || "";
                            employeeUsersState = employeeUsersState.map((item) => (
                                item.email === email ? { ...item, role: select.value === "admin" ? "admin" : "user", role_label: select.value === "admin" ? "Admin" : "User" } : item
                            ));
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                        });
                    });

                    employeeTableBody.querySelectorAll("[data-employee-remove]").forEach((button) => {
                        button.addEventListener("click", () => {
                            const email = button.getAttribute("data-employee-remove") || "";
                            employeeUsersState = employeeUsersState.filter((item) => item.email !== email);
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                        });
                    });
                };

                const saveEmployeeUsers = async () => {
                    try {
                        const response = await fetch("/admin/save-users", {
                            method: "POST",
                            headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
                            body: JSON.stringify({ users: employeeUsersState.map((item) => ({ email: item.email, role: item.role })) }),
                        });
                        const data = await response.json();
                        if (data.ok && Array.isArray(data.users)) {
                            employeeUsersState = dedupeEmployees(data.users);
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                        }
                        showNotice(
                            data.message || (data.ok ? "Đã lưu danh sách nhân viên." : "Không lưu được danh sách nhân viên."),
                            data.level || (data.ok ? "success" : "error")
                        );
                    } catch (_) {
                        showNotice("Không lưu được danh sách nhân viên. Vui lòng thử lại.", "error");
                    }
                };

                if (employeeUsersData) {
                    employeeUsersState = dedupeEmployees(parseEmployeeUsersData());
                    updateEmployeeSummary(employeeUsersState);
                    renderEmployeeRows();
                }

                if (employeeStatusChips.length) {
                    employeeStatusChips.forEach((chip) => {
                        chip.addEventListener("click", () => {
                            employeeStatusFilter = chip.getAttribute("data-employee-status") || "all";
                            employeeStatusChips.forEach((item) => item.classList.toggle("is-active", item === chip));
                            renderEmployeeRows();
                        });
                    });
                }

                if (employeeSearchInput) {
                    employeeSearchInput.addEventListener("input", renderEmployeeRows);
                }

                if (employeeRoleFilter) {
                    employeeRoleFilter.addEventListener("change", renderEmployeeRows);
                }

                if (employeeAddBtn) {
                    employeeAddBtn.addEventListener("click", () => {
                        const email = String(employeeEmailInput?.value || "").trim().toLowerCase();
                        const role = String(employeeRoleInput?.value || "user").trim().toLowerCase() === "admin" ? "admin" : "user";
                        if (!email || !email.includes("@")) {
                            showNotice("Email nhân viên không hợp lệ.", "error");
                            return;
                        }
                        const existing = employeeUsersState.find((item) => item.email === email);
                        if (existing) {
                            employeeUsersState = employeeUsersState.map((item) => (
                                item.email === email ? { ...item, role, role_label: role === "admin" ? "Admin" : "User" } : item
                            ));
                            showNotice("Đã cập nhật role cho email đã tồn tại.", "info");
                        } else {
                            employeeUsersState = dedupeEmployees([
                                ...employeeUsersState,
                                {
                                    email,
                                    role,
                                    role_label: role === "admin" ? "Admin" : "User",
                                    status_key: "pending",
                                    status_label: "Chờ xác thực",
                                    last_login_text: "Chưa có",
                                    login_count: 0,
                                    is_forced_admin: false,
                                },
                            ]);
                            showNotice("Đã thêm nhân viên vào danh sách chờ lưu.", "success");
                        }
                        if (employeeEmailInput) employeeEmailInput.value = "";
                        if (employeeRoleInput) employeeRoleInput.value = "user";
                        updateEmployeeSummary(employeeUsersState);
                        renderEmployeeRows();
                    });
                }

                if (employeeSaveBtn) {
                    employeeSaveBtn.addEventListener("click", saveEmployeeUsers);
                }

                if (employeeResetBtn) {
                    employeeResetBtn.addEventListener("click", () => {
                        if (employeeSearchInput) employeeSearchInput.value = "";
                        if (employeeRoleFilter) employeeRoleFilter.value = "all";
                        employeeStatusFilter = "all";
                        employeeStatusChips.forEach((chip) => chip.classList.toggle("is-active", (chip.getAttribute("data-employee-status") || "all") === "all"));
                        renderEmployeeRows();
                    });
                }

                if (employeeImportBtn && employeeImportInput) {
                    employeeImportBtn.addEventListener("click", () => employeeImportInput.click());
                    employeeImportInput.addEventListener("change", async () => {
                        const file = employeeImportInput.files?.[0];
                        if (!file) return;
                        try {
                            const raw = await file.text();
                            const imported = raw
                                .split(/?
/)
                                .map((line) => line.trim())
                                .filter(Boolean)
                                .map((line) => {
                                    const parts = line.split(/[,	;]+/).map((part) => part.trim()).filter(Boolean);
                                    return {
                                        email: parts[0] || "",
                                        role: String(parts[1] || "user").toLowerCase() === "admin" ? "admin" : "user",
                                    };
                                });
                            employeeUsersState = dedupeEmployees([...employeeUsersState, ...imported]);
                            updateEmployeeSummary(employeeUsersState);
                            renderEmployeeRows();
                            showNotice(`Đã nhập ${imported.length} dòng nhân viên từ file.`, "success");
                        } catch (_) {
                            showNotice("Không đọc được file nhân viên. Dùng CSV hoặc TXT đơn giản.", "error");
                        } finally {
                            employeeImportInput.value = "";
                        }
                    });
                }

                const applyActiveSheetMeta = (data, syncInputs = false) => {
                    const sheetName = (data?.active_sheet_name || "").trim() || "Chưa cài đặt";
                    const sheetId = (data?.active_sheet_id || "").trim() || "Chưa cài đặt";
                    activeSheetNameEls.forEach((el) => {
                        el.textContent = sheetName;
                    });
                    activeSheetIdEls.forEach((el) => {
                        el.textContent = sheetId;
                    });
                    if (syncInputs && sheetNameInput && typeof data?.active_sheet_name === "string") {
                        sheetNameInput.value = data.active_sheet_name;
                    }
                    if (syncInputs && sheetUrlInput && typeof data?.snapshot_url === "string") {
                        sheetUrlInput.value = data.snapshot_url;
                    }
                };

                const applyColumnConfigState = (data) => {
                    const columnConfig = data?.column_config;
                    if (!columnConfig) return;
                    const manualMode = columnConfig.manual_mode || "AUTO";
                    configModeEls.forEach((el) => {
                        el.textContent = manualMode;
                    });
                    const metricCols = columnConfig.metric_cols || {};
                    Object.entries(metricColEls).forEach(([field, elements]) => {
                        const value = metricCols[field] || "-";
                        elements.forEach((el) => {
                            el.textContent = value;
                        });
                    });
                    const startRow = `${columnConfig.start_row || 2}`;
                    startRowEls.forEach((el) => {
                        el.textContent = startRow;
                    });
                    const detectedText = columnConfig.detected_text || "";
                    columnDetectedTextEls.forEach((el) => {
                        el.textContent = detectedText;
                    });
                };

                const applyScheduleConfigState = (data) => {
                    const scheduleConfig = data?.schedule_config;
                    if (!scheduleConfig) return;
                    const label = scheduleConfig.label || "Chưa bật";
                    scheduleLabelEls.forEach((el) => {
                        el.textContent = label;
                    });
                    if (scheduleBoundSheetName) {
                        scheduleBoundSheetName.textContent = scheduleConfig.sheet_name_text || "Chưa chốt tab nào";
                    }
                    if (scheduleBoundSheetId) {
                        scheduleBoundSheetId.textContent = scheduleConfig.sheet_id_text || "Chưa có Spreadsheet ID";
                    }
                    if (scheduleBoundScope) {
                        scheduleBoundScope.textContent = scheduleConfig.scope_text || "";
                    }
                    if (scheduleBoundLink) {
                        const hasLink = Boolean(scheduleConfig.snapshot_url);
                        scheduleBoundLink.classList.toggle("hidden", !hasLink);
                        scheduleBoundLink.href = hasLink ? scheduleConfig.snapshot_url : "#";
                    }
                };

                const applyScheduleTrackingState = (data) => {
                    const tracking = data?.schedule_tracking;
                    if (!tracking) return;
                    if (scheduleTrackNext) {
                        scheduleTrackNext.textContent = tracking.next_run_text || "Chưa có";
                    }
                    if (scheduleTrackStarted) {
                        scheduleTrackStarted.textContent = tracking.last_started_text || "Chưa có";
                    }
                    if (scheduleTrackFinished) {
                        scheduleTrackFinished.textContent = tracking.last_finished_text || "Chưa có";
                    }
                    if (scheduleTrackDuration) {
                        scheduleTrackDuration.textContent = tracking.last_duration_text || "0s";
                    }
                    if (scheduleTrackRunning) {
                        scheduleTrackRunning.textContent = tracking.is_running_text || "Đang chờ";
                    }
                    if (scheduleTrackStatus) {
                        scheduleTrackStatus.textContent = tracking.last_status_text || "Chưa chạy";
                    }
                    if (scheduleTrackSource) {
                        scheduleTrackSource.textContent = tracking.last_source_text || "Chưa có";
                    }
                    if (scheduleTrackSheet) {
                        scheduleTrackSheet.textContent = tracking.last_sheet_text || "Chưa có";
                    }
                    if (scheduleTrackProcessed) {
                        scheduleTrackProcessed.textContent = tracking.last_processed_text || "0";
                    }
                    if (scheduleTrackSuccess) {
                        scheduleTrackSuccess.textContent = tracking.last_success_text || "0";
                    }
                    if (scheduleTrackFailed) {
                        scheduleTrackFailed.textContent = tracking.last_failed_text || "0";
                    }
                    if (scheduleTrackHistory && typeof tracking.history_html === "string") {
                        scheduleTrackHistory.innerHTML = tracking.history_html;
                    }
                };

                const applyStatusState = (data) => {
                    if (!data) return;
                    if (statusBadge) {
                        statusBadge.className = data.status_badge_class;
                        statusBadge.textContent = data.status_badge_text;
                    }
                    if (sidebarStatusText) {
                        sidebarStatusText.textContent = data.status_badge_text;
                    }
                    if (sidebarStatusTask) {
                        sidebarStatusTask.textContent = data.current_task;
                    }
                    if (currentTaskLabel) {
                        currentTaskLabel.textContent = data.current_task;
                    }
                    if (progressBar) {
                        progressBar.style.width = data.progress_width;
                    }
                    if (logSection && typeof data.log_html === "string") {
                        logSection.innerHTML = data.log_html;
                    }
                    if (primaryAction && typeof data.primary_action_html === "string") {
                        primaryAction.innerHTML = data.primary_action_html;
                    }
                };

                const refreshDashboard = async () => {
                    if (document.hidden || refreshInFlight) return;
                    refreshInFlight = true;
                    try {
                        const response = await fetch("/status", {
                            headers: { "X-Requested-With": "fetch" },
                            cache: "no-store",
                        });
                        if (!response.ok) return;
                        const data = await response.json();
                        applyStatusState(data);
                        applyActiveSheetMeta(data);
                        applyColumnConfigState(data);
                        applyScheduleConfigState(data);
                        applyScheduleTrackingState(data);
                    } catch (_) {
                    } finally {
                        refreshInFlight = false;
                    }
                };

                const getVisibleRowChecks = (panel) => Array.from(panel.querySelectorAll(".post-row"))
                    .filter((row) => !row.classList.contains("hidden"))
                    .map((row) => row.querySelector("[data-post-select]"))
                    .filter(Boolean);

                const updatePanelSelectAllState = (panel) => {
                    if (!panel) return;
                    const selectAll = panel.querySelector("[data-select-all-posts]");
                    if (!selectAll) return;
                    const rowChecks = getVisibleRowChecks(panel);
                    const checkedCount = rowChecks.filter((item) => item.checked).length;
                    selectAll.checked = rowChecks.length > 0 && checkedCount === rowChecks.length;
                    selectAll.indeterminate = checkedCount > 0 && checkedCount < rowChecks.length;
                };

                const syncPostsSelectionState = () => {
                    postsTabPanels.forEach((panel) => updatePanelSelectAllState(panel));
                };

                const getActivePostsPanel = () => postsTabPanels.find((panel) => panel.classList.contains("is-active")) || null;

                const applyPostFilters = (panel = getActivePostsPanel()) => {
                    if (!panel) {
                        if (postsVisibleCount) {
                            postsVisibleCount.textContent = "0 bài";
                        }
                        return;
                    }
                    const searchInput = panel.querySelector(".posts-search-field");
                    const emptyState = panel.querySelector(".posts-empty-panel");
                    const term = (searchInput?.value || "").trim().toLowerCase();
                    const activePlatform = panel.dataset.postsPlatform || "all";
                    const rows = Array.from(panel.querySelectorAll(".post-row"));
                    let visible = 0;

                    rows.forEach((row) => {
                        const platform = row.dataset.platform || "khac";
                        const haystack = row.dataset.search || "";
                        const matchesPlatform = activePlatform === "all" || platform === activePlatform;
                        const matchesTerm = !term || haystack.includes(term);
                        const shouldShow = matchesPlatform && matchesTerm;
                        row.classList.toggle("hidden", !shouldShow);
                        if (shouldShow) {
                            visible += 1;
                        }
                    });

                    if (emptyState) {
                        emptyState.classList.toggle("hidden", visible !== 0);
                    }
                    if (postsVisibleCount) {
                        postsVisibleCount.textContent = `${visible} bài`;
                    }
                    if (postsActiveTabLabel) {
                        postsActiveTabLabel.textContent = panel.dataset.postsTabTitle || "Chưa chọn";
                    }
                    updatePanelSelectAllState(panel);
                };

                const setActivePostsTab = (tabSlug) => {
                    const safeSlug = postsTabCards.some((card) => card.dataset.postsTabTrigger === tabSlug)
                        ? tabSlug
                        : (postsTabCards[0]?.dataset.postsTabTrigger || "");
                    postsTabCards.forEach((card) => {
                        card.classList.toggle("is-active", card.dataset.postsTabTrigger === safeSlug);
                    });
                    postsTabPanels.forEach((panel) => {
                        panel.classList.toggle("is-active", panel.dataset.postsTabPanel === safeSlug);
                    });
                    applyPostFilters(getActivePostsPanel());
                };

                postsTabCards.forEach((card) => {
                    card.addEventListener("click", () => {
                        setActivePostsTab(card.dataset.postsTabTrigger || "");
                    });
                });

                postsTabPanels.forEach((panel) => {
                    const searchField = panel.querySelector(".posts-search-field");
                    const resetButton = panel.querySelector(".posts-reset-btn");
                    const chips = Array.from(panel.querySelectorAll(".posts-chip"));
                    const selectAll = panel.querySelector("[data-select-all-posts]");
                    const rowChecks = Array.from(panel.querySelectorAll("[data-post-select]"));

                    chips.forEach((chip) => {
                        chip.addEventListener("click", () => {
                            panel.dataset.postsPlatform = chip.dataset.platform || "all";
                            chips.forEach((item) => item.classList.toggle("is-active", item === chip));
                            applyPostFilters(panel);
                        });
                    });

                    if (searchField) {
                        searchField.addEventListener("input", () => applyPostFilters(panel));
                    }

                    if (resetButton) {
                        resetButton.addEventListener("click", () => {
                            panel.dataset.postsPlatform = "all";
                            if (searchField) {
                                searchField.value = "";
                            }
                            chips.forEach((chip) => chip.classList.toggle("is-active", (chip.dataset.platform || "all") === "all"));
                            applyPostFilters(panel);
                        });
                    }

                    if (selectAll) {
                        selectAll.addEventListener("change", () => {
                            getVisibleRowChecks(panel).forEach((checkbox) => {
                                checkbox.checked = selectAll.checked;
                            });
                            syncPostsSelectionState();
                        });
                    }

                    rowChecks.forEach((checkbox) => {
                        checkbox.addEventListener("change", () => {
                            syncPostsSelectionState();
                        });
                    });
                });

                const setActivePanel = (sectionId) => {
                    const availableIds = dashboardSections.map((section) => section.dataset.dashboardSection);
                    const targetId = availableIds.includes(sectionId) ? sectionId : "tong-quan";

                    sidebarLinks.forEach((link) => {
                        link.classList.toggle("is-active", link.dataset.navLink === targetId);
                    });
                    dashboardSections.forEach((section) => {
                        section.classList.toggle("is-active", section.dataset.dashboardSection === targetId);
                    });

                    if (window.location.hash !== `#${targetId}`) {
                        history.replaceState(null, "", `#${targetId}`);
                    }
                };

                sidebarLinks.forEach((link) => {
                    link.addEventListener("click", (event) => {
                        event.preventDefault();
                        setActivePanel(link.dataset.navLink || "tong-quan");
                    });
                });

                window.addEventListener("hashchange", () => {
                    setActivePanel((window.location.hash || "").replace("#", ""));
                });

                if (postsTabCards.length) {
                    const initialPostsTab = postsTabCards.find((card) => card.classList.contains("is-active"))?.dataset.postsTabTrigger
                        || postsTabCards[0].dataset.postsTabTrigger;
                    setActivePostsTab(initialPostsTab || "");
                }
                syncPostsSelectionState();
                setActivePanel((window.location.hash || "").replace("#", "") || "tong-quan");
                setInterval(refreshDashboard, 4000);
            });
        