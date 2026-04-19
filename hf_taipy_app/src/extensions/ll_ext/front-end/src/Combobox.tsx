/*
 * Combobox — WAI-ARIA APG combobox-with-list-autocomplete.
 *
 * Wraps MUI Autocomplete. Replaces the provisional
 * `SidebarWidget(searchable=True)` pattern (debounced |input| + always-visible
 * non-dropdown |selector|) with an accessible, reusable custom element.
 *
 * APG reference: https://www.w3.org/WAI/ARIA/apg/patterns/combobox/examples/
 * combobox-autocomplete-list/
 *
 * Visibility rules (APG):
 *   - Listbox HIDDEN when: input empty AND not focused
 *   - Listbox HIDDEN on: Escape, selection, Tab/click-away
 *   - Listbox VISIBLE on: focus with non-empty input, ArrowDown, typing
 *
 * ARIA wiring is handled by MUI Autocomplete; we verify it in staging via
 * Puppeteer (role=combobox on the input, role=listbox on the popper,
 * aria-controls + aria-expanded + aria-activedescendant).
 */
import React, {
    useCallback,
    useEffect,
    useMemo,
    useRef,
    useState,
} from "react";
import {
    createSendUpdateAction,
    getUpdateVar,
    LoV,
    LovItem,
    useDispatch,
    useDynamicProperty,
    useLovListMemo,
    useModule,
} from "taipy-gui";
import Autocomplete, {
    AutocompleteCloseReason,
    AutocompleteInputChangeReason,
    AutocompleteRenderInputParams,
} from "@mui/material/Autocomplete";
import TextField from "@mui/material/TextField";

// Matches `filters.NO_MATCHES_SENTINEL` on the Python side.
const NO_MATCHES_SENTINEL = "(no matches)";

interface ComboboxProps {
    // Taipy builder-injected identity / binding props.
    id?: string;
    updateVarName?: string;
    updateVars?: string;
    propagate?: boolean;

    // Default property: selected label (two-way bound). Taipy's wire format
    // for `lov_value` can arrive as either a plain string or a single-item
    // list; `currentValue` below normalizes both.
    value?: string | string[];
    defaultValue?: string | string[];

    // LOV (list of option labels). Taipy serializes list[str] to LoVElt[].
    lov?: LoV;
    defaultLov?: string;

    // Search-input text (two-way bound; debounced dispatch).
    search?: string;
    defaultSearch?: string;

    // Static UX.
    label?: string;
    defaultLabel?: string;
    placeholder?: string;
    defaultPlaceholder?: string;
    noMatchesLabel?: string;
    defaultNoMatchesLabel?: string;

    // Layout / behaviour.
    className?: string;
    defaultClassName?: string;
    required?: boolean;
    debounceMs?: number;

    // Taipy callback names (strings).
    onChange?: string;
    onSearch?: string;
}

/* -- Debounce helper -------------------------------------------------- */

function useDebouncedCallback<A extends unknown[]>(
    fn: (...args: A) => void,
    delay: number,
): (...args: A) => void {
    const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const fnRef = useRef(fn);
    fnRef.current = fn;
    useEffect(() => () => {
        if (timerRef.current) clearTimeout(timerRef.current);
    }, []);
    return useCallback((...args: A) => {
        if (timerRef.current) clearTimeout(timerRef.current);
        timerRef.current = setTimeout(() => fnRef.current(...args), delay);
    }, [delay]);
}

/* -- Combobox --------------------------------------------------------- */

export default function Combobox(props: ComboboxProps) {
    const {
        id,
        updateVarName,
        updateVars = "",
        propagate = true,
        value,
        defaultValue = "",
        lov,
        defaultLov = "",
        search,
        defaultSearch = "",
        label,
        defaultLabel = "",
        placeholder,
        defaultPlaceholder = "Type to search\u2026",
        noMatchesLabel,
        defaultNoMatchesLabel = "No matches",
        className,
        defaultClassName = "",
        required = true,
        debounceMs = 300,
        onChange = "",
        onSearch = "",
    } = props;

    const dispatch = useDispatch();
    const moduleName = useModule();

    // Dynamic-property hooks: server-pushed value + static fallback.
    // `lov_value` can surface as a list-wrapped sentinel on first render
    // (Taipy's native shape for LOV controls); unwrap to plain string.
    const rawValue = useDynamicProperty<string | string[] | undefined>(
        value as string | string[] | undefined,
        defaultValue as unknown as string | string[] | undefined,
        "",
    );
    const currentValue = useMemo(() => {
        if (Array.isArray(rawValue)) {
            return rawValue.length > 0 ? String(rawValue[0] ?? "") : "";
        }
        const s = (rawValue ?? "") as string;
        // Guard against Taipy's serialized-JSON sentinels for empty
        // lov_value defaults (observed as "[]" and "[\"\"]" on first render).
        if (typeof s === "string" && s.length > 0 && s[0] === "[") {
            try {
                const parsed = JSON.parse(s);
                if (Array.isArray(parsed)) {
                    return parsed.length > 0 ? String(parsed[0] ?? "") : "";
                }
            } catch {
                /* not JSON — fall through */
            }
        }
        return s;
    }, [rawValue]);
    const currentSearch = useDynamicProperty(search, defaultSearch, "");
    const currentLabel = useDynamicProperty(label, defaultLabel, "");
    const currentPlaceholder = useDynamicProperty(
        placeholder,
        defaultPlaceholder,
        "Type to search\u2026",
    );
    const currentNoMatches = useDynamicProperty(
        noMatchesLabel,
        defaultNoMatchesLabel,
        "No matches",
    );
    const currentClassName = useDynamicProperty(className, defaultClassName, "");

    // LOV items (parse LoVElt[] -> LovItem[]). We KEEP the NO_MATCHES
    // sentinel in the list so MUI renders its popper (empty options would
    // silently close) — it's shown disabled with the `noMatchesLabel`
    // string via `getOptionLabel` / `getOptionDisabled` below.
    const lovItems = useLovListMemo(lov, defaultLov, false);

    // Local input state. `inputValue` is fully controlled; `value` is NOT
    // passed to MUI, to sidestep MUI's behaviour of resetting inputValue to
    // getOptionLabel(value) on every render (which would wipe the user's
    // typed text mid-search whenever the parent re-renders). The currently-
    // selected label is tracked via `lastServerValueRef` and echoed back
    // into `inputValue` only when the server pushes a new selection or a
    // search reset.
    const [inputValue, setInputValue] = useState(currentValue ?? "");
    const lastServerValueRef = useRef<string>(currentValue ?? "");
    const lastServerSearchRef = useRef<string>(currentSearch ?? "");

    useEffect(() => {
        // Server pushed a new selected label (e.g. URL load, competition
        // change) — echo into the input for display.
        if (currentValue !== lastServerValueRef.current) {
            lastServerValueRef.current = currentValue ?? "";
            setInputValue(currentValue ?? "");
        }
    }, [currentValue]);

    useEffect(() => {
        // Server pushed a search reset (e.g. competition change clears the
        // search). Echo only when it's a real reset — not the echo of the
        // value we just dispatched, which is always equal to the last
        // known server search.
        if (currentSearch !== lastServerSearchRef.current) {
            lastServerSearchRef.current = currentSearch ?? "";
            if ((currentSearch ?? "") === "") {
                setInputValue(currentValue ?? "");
            }
        }
    }, [currentSearch, currentValue]);

    // Backend variable names for dispatches.
    const valueVarName = updateVarName;
    const searchVarName = useMemo(
        () => getUpdateVar(updateVars, "search"),
        [updateVars],
    );

    // Label decoration for optional fields.
    const effectiveLabel = useMemo(() => {
        const base = currentLabel ?? "";
        return !required && base ? `${base} (optional)` : base;
    }, [currentLabel, required]);

    // Debounced search dispatch (text -> backend var + on_search callback).
    const dispatchSearch = useCallback(
        (text: string) => {
            if (!searchVarName) return;
            dispatch(
                createSendUpdateAction(
                    searchVarName,
                    text,
                    moduleName,
                    onSearch || undefined,
                    propagate,
                ),
            );
        },
        [dispatch, moduleName, onSearch, propagate, searchVarName],
    );
    const debouncedDispatchSearch = useDebouncedCallback(
        dispatchSearch,
        debounceMs,
    );

    const handleInputChange = useCallback(
        (
            _e: React.SyntheticEvent,
            text: string,
            reason: AutocompleteInputChangeReason,
        ) => {
            // Ignore MUI's internal 'reset' events entirely — we control
            // sync-from-server through the useEffect pair above. Writing the
            // selected label back into `inputValue` on every render would
            // clobber the user's active typing.
            if (reason === "reset") return;
            setInputValue(text);
            if (reason === "input" || reason === "clear") {
                debouncedDispatchSearch(text);
            }
        },
        [debouncedDispatchSearch],
    );

    const handleChange = useCallback(
        (_e: React.SyntheticEvent, next: LovItem | string | null) => {
            const selected =
                next == null
                    ? ""
                    : typeof next === "string"
                        ? next
                        : next.id;
            if (selected === NO_MATCHES_SENTINEL) return;
            // Display the selected label immediately — don't wait for the
            // server echo.
            setInputValue(selected);
            lastServerValueRef.current = selected;
            if (valueVarName) {
                dispatch(
                    createSendUpdateAction(
                        valueVarName,
                        selected,
                        moduleName,
                        onChange || undefined,
                        propagate,
                    ),
                );
            }
        },
        [dispatch, moduleName, onChange, propagate, valueVarName],
    );

    /* -- MUI plumbing ------------------------------------------------- */

    const getOptionLabel = useCallback(
        (o: LovItem | string): string => {
            const label =
                typeof o === "string"
                    ? o
                    : typeof o.item === "string"
                        ? o.item
                        : o.id;
            // Render the backend's "no matches" sentinel with the user-
            // facing label (e.g. "No matches") so the popper shows a
            // readable disabled row instead of the raw "(no matches)".
            if (label === NO_MATCHES_SENTINEL) {
                return currentNoMatches ?? "No matches";
            }
            return label;
        },
        [currentNoMatches],
    );

    const getOptionDisabled = useCallback(
        (o: LovItem | string): boolean => {
            const optId = typeof o === "string" ? o : o.id;
            return optId === NO_MATCHES_SENTINEL;
        },
        [],
    );

    const isOptionEqualToValue = useCallback(
        (opt: LovItem | string, val: LovItem | string): boolean => {
            const optId = typeof opt === "string" ? opt : opt.id;
            const valId = typeof val === "string" ? val : val.id;
            return optId === valId;
        },
        [],
    );

    // Restore the input to the currently-selected label when the user
    // dismisses the listbox without selecting (Escape, blur, click-away,
    // clear button). APG allows either behaviour; the "restore" variant
    // matches user expectation that typing is ephemeral until committed.
    const handleClose = useCallback(
        (_e: React.SyntheticEvent, reason: AutocompleteCloseReason) => {
            if (reason === "selectOption" || reason === "createOption") return;
            setInputValue(lastServerValueRef.current);
        },
        [],
    );

    const renderInput = useCallback(
        (params: AutocompleteRenderInputParams) => (
            <TextField
                {...params}
                label={effectiveLabel}
                placeholder={currentPlaceholder}
                size="small"
                fullWidth
            />
        ),
        [effectiveLabel, currentPlaceholder],
    );

    return (
        <Autocomplete
            id={id}
            className={currentClassName}
            options={lovItems}
            inputValue={inputValue}
            onChange={handleChange}
            onClose={handleClose}
            onInputChange={handleInputChange}
            getOptionLabel={getOptionLabel}
            getOptionDisabled={getOptionDisabled}
            isOptionEqualToValue={isOptionEqualToValue}
            renderInput={renderInput}
            freeSolo={false}
            autoHighlight
            noOptionsText={currentNoMatches}
            size="small"
            blurOnSelect
            filterOptions={(opts) => opts /* server-side search, no client filter */}
        />
    );
}
