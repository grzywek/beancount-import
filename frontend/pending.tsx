import * as React from "react";
import * as ReactDOM from "react-dom";
import styled from "styled-components";
import scrollIntoView from "scroll-into-view-if-needed";
import { PendingEntry, ServerListCache } from "./server_connection";
import { VirtualList, VirtualListScrollState } from "./virtual_list";
import { EventEmitter, EventSubscription } from "fbemitter";
import {
  ServerVirtualListComponent,
  ServerVirtualListState
} from "./server_virtual_list";

class PendingVirtualListComponent extends ServerVirtualListComponent<
  PendingEntry
> { }

const PendingFilterWrapper = styled.div`
  padding: 8px 10px;
  border-bottom: 1px solid var(--color-main-accent);
  display: flex;
  gap: 8px;
  align-items: center;
  box-sizing: border-box;
`;

const PendingFilterInput = styled.input`
  flex: 1;
  padding: 7px 6px 8px;
  border: 1px solid var(--color-main-accent);
  border-radius: 5px;
  background-color: var(--color-main-bg);
  color: var(--color-main-text);
  font-family: var(--font-fam-sans);
  font-size: var(--font-size-sans-reg);
  outline: none;

  &:focus {
    border-color: var(--color-link-text);
    box-shadow: 0 0 0 2px hsla(229, 86%, 42%, 0.2);
  }

  &::placeholder {
    color: var(--color-main-accent);
  }
`;

const PendingFilterCount = styled.span`
  font-size: var(--font-size-sans-small);
  color: var(--color-main-accent);
  white-space: nowrap;
`;

// Separate scrollable container for filtered results (bypasses virtual list)
const FilteredListElement = styled.div`
  overflow-y: scroll;
  flex: 1;
  flex-basis: 0px;
`;

const PendingEntryListElement = styled(PendingVirtualListComponent)`
  overflow-y: scroll;
  flex: 1;
  flex-basis: 0px;
  padding: 8px 0;
`;

const PendingEntryElement = styled.div<
  { selected: boolean; highlighted: boolean; hidden?: boolean }>`
  cursor: pointer;
  font-size: var(--font-size-sans-small);
  padding: 12px 8px;
  border-bottom: 1px solid var(--color-main-accent);
  min-width: 100%;
  box-sizing: border-box;
  ${props => props.hidden && `display: none;`}
  ${props => (props.highlighted &&
    `
    background-color: var(--color-hover-bg);
    color: var(--color-hover-text);
    `
  )};
  ${props => (props.selected &&
    `
    background-color: var(--color-select-bg);
    color: var(--color-select-text);
    `
  )};
`;

const PendingEntryFormattedElement = styled.div`
  font-family: var(--font-fam-mono);
  font-size: var(--font-size-mono-reg);
  white-space: pre;
`;

const PendingEntrySourceNameElement = styled.div`
  border-top: 1px solid var(--color-main-accent);
  margin: 6px 0 2px;
  padding: 6px 0 0px;
  white-space: nowrap;
`;

const PendingEntrySourceFilenameElement = styled.div`
  white-space: nowrap;
`;

const PendingEntryInfoElement = styled.div`
  text-align: center;
`;

export class PendingEntryHighlightState {
  emitter = new EventEmitter();
  index?: number = undefined;

  set(index: number) {
    if (index !== this.index) {
      this.index = index;
      this.emitter.emit("set");
    }
  }
}

class PendingEntryComponent extends React.PureComponent<{
  entry: PendingEntry;
  selected: boolean;
  highlighted: boolean;
  index: number;
  onSelect: (index: number) => void;
  onHover: (index?: number) => void;
}> {
  render() {
    const { entry } = this.props;
    let filename: string | undefined;
    let lineno: number | undefined;
    let source = entry.source;
    if (source != null) {
      if (entry.info != null && entry.info.filename != null) {
        filename = entry.info.filename;
        if (entry.info.line != null) {
          lineno = entry.info.line;
        }
      }
    } else {
      source = "fixme";
      const meta = entry.entries[0].meta;
      if (meta != null && meta["filename"] != null) {
        filename = meta["filename"];
        if (meta["lineno"] != null) {
          lineno = meta["lineno"];
        }
      }
    }
    return (
      <PendingEntryElement
        onClick={this.handleSelect}
        selected={this.props.selected}
        highlighted={this.props.highlighted}
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
      >
        <PendingEntryFormattedElement>
          {entry.formatted.trim()}
        </PendingEntryFormattedElement>
        {this.props.selected && (
          <PendingEntrySourceNameElement>
            <em>Source:</em> {source}
          </PendingEntrySourceNameElement>
        )}
        {this.props.selected && filename && (
          <PendingEntrySourceFilenameElement>
            <em>File:</em> {filename}
            {lineno != undefined && `:${lineno}`}
          </PendingEntrySourceFilenameElement>
        )}
      </PendingEntryElement>
    );
  }

  private handleMouseEnter = () => {
    this.props.onHover(this.props.index);
  };

  private handleMouseLeave = () => {
    this.props.onHover(undefined);
  };

  private handleSelect = () => {
    this.props.onSelect(this.props.index);
  };
}

interface PendingEntriesComponentProps {
  listState: ServerVirtualListState<PendingEntry>;
  onSelect: (index: number) => void;
  selectedIndex?: number;
  highlightState: PendingEntryHighlightState;
}

interface PendingEntriesComponentState {
  highlightedIndex?: number;
  filterText: string;
  cachedEntries: Map<number, PendingEntry>;
  lastGeneration: number;
}

export class PendingEntriesComponent extends React.PureComponent<
  PendingEntriesComponentProps,
  PendingEntriesComponentState
> {
  state: PendingEntriesComponentState = {
    highlightedIndex: this.props.highlightState.index,
    filterText: "",
    cachedEntries: new Map(),
    lastGeneration: -1
  };

  selectedRef = React.createRef<HTMLElement>();
  highlightedRef = React.createRef<HTMLElement>();
  filterInputRef = React.createRef<HTMLInputElement>();

  // Check if entry matches filter - only searches in payee and narration
  private matchesFilter = (entry: PendingEntry): boolean => {
    const { filterText } = this.state;
    if (!filterText.trim()) {
      return true;
    }
    const searchText = filterText.toLowerCase();

    // Search only in entries' payee and narration
    for (const e of entry.entries) {
      if ('payee' in e && e.payee && e.payee.toLowerCase().includes(searchText)) {
        return true;
      }
      if ('narration' in e && e.narration && e.narration.toLowerCase().includes(searchText)) {
        return true;
      }
    }

    return false;
  };

  // Get list length from metadata
  private getListLength = (): number => {
    const metadata = this.props.listState.metadata;
    return metadata ? metadata[1] : 0;
  };

  // Get generation from metadata
  private getGeneration = (): number => {
    const metadata = this.props.listState.metadata;
    return metadata ? metadata[0] : -1;
  };

  // Get all filtered entries from cache, clearing cache if generation changed
  private getFilteredEntries = (): Array<{ entry: PendingEntry, index: number }> => {
    const currentGeneration = this.getGeneration();
    const { cachedEntries, lastGeneration } = this.state;

    // Clear cache if generation changed (e.g., after accept/ignore)
    if (currentGeneration !== lastGeneration && lastGeneration !== -1) {
      this.setState({
        cachedEntries: new Map(),
        lastGeneration: currentGeneration
      });
      return [];
    }

    // Update generation tracking if first time
    if (lastGeneration === -1 && currentGeneration !== -1) {
      this.setState({ lastGeneration: currentGeneration });
    }

    const result: Array<{ entry: PendingEntry, index: number }> = [];

    // Sort by index to maintain order
    const sortedIndices = Array.from(cachedEntries.keys()).sort((a, b) => a - b);

    for (const index of sortedIndices) {
      const entry = cachedEntries.get(index);
      if (entry && this.matchesFilter(entry)) {
        result.push({ entry, index });
      }
    }

    return result;
  };

  private renderItem = (
    entry: PendingEntry,
    index: number,
    ref: React.RefObject<any>
  ) => {
    const { selectedIndex } = this.props;
    const { highlightedIndex } = this.state;

    // Cache the entry when we see it
    if (!this.state.cachedEntries.has(index)) {
      this.state.cachedEntries.set(index, entry);
    }

    return (
      <PendingEntryComponent
        selected={index === selectedIndex}
        key={index}
        ref={ref}
        entry={entry}
        index={index}
        onSelect={this.props.onSelect}
        onHover={this.handleHover}
        highlighted={index === highlightedIndex}
      />
    );
  };

  private renderFilteredItem = (entry: PendingEntry, index: number) => {
    const { selectedIndex } = this.props;
    const { highlightedIndex } = this.state;

    return (
      <PendingEntryComponent
        selected={index === selectedIndex}
        key={index}
        entry={entry}
        index={index}
        onSelect={this.props.onSelect}
        onHover={this.handleHover}
        highlighted={index === highlightedIndex}
      />
    );
  };

  private handleFilterChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const newFilterText = event.target.value;

    // When filter becomes active, request all items to cache them
    if (newFilterText && !this.state.filterText) {
      const length = this.getListLength();
      const generation = this.getGeneration();
      if (length > 0) {
        this.props.listState.cache.requestRange(generation, length, 0, length);
      }
    }

    this.setState({ filterText: newFilterText });
  };

  private handleFilterKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Escape") {
      this.setState({ filterText: "" });
      this.filterInputRef.current?.blur();
      event.preventDefault();
    }
  };

  private handleKeyDown = (event: KeyboardEvent) => {
    // Don't handle if already in an input
    if (
      event.target instanceof HTMLInputElement ||
      event.target instanceof HTMLTextAreaElement
    ) {
      return;
    }

    if (event.key === "/") {
      event.preventDefault();
      this.filterInputRef.current?.focus();
    }
  };

  render() {
    const { filterText } = this.state;
    const isFiltering = filterText.trim().length > 0;

    // Get filtered entries for display
    const filteredEntries = isFiltering ? this.getFilteredEntries() : [];
    const totalCount = this.getListLength();

    return (
      <>
        <PendingFilterWrapper>
          <PendingFilterInput
            ref={this.filterInputRef}
            type="text"
            placeholder="Filter pending entries... (press / to focus)"
            value={filterText}
            onChange={this.handleFilterChange}
            onKeyDown={this.handleFilterKeyDown}
          />
          {isFiltering && (
            <PendingFilterCount>
              {filteredEntries.length} / {totalCount}
            </PendingFilterCount>
          )}
        </PendingFilterWrapper>
        {isFiltering ? (
          <FilteredListElement>
            {filteredEntries.map(({ entry, index }) =>
              this.renderFilteredItem(entry, index)
            )}
          </FilteredListElement>
        ) : (
          <PendingEntryListElement
            listState={this.props.listState}
            renderItem={this.renderItem.bind(this)}
          />
        )}
      </>
    );
  }

  highlightStateSubscription?: EventSubscription;
  metadataSubscription?: EventSubscription;

  componentDidMount() {
    this.highlightStateSubscription = this.props.highlightState.emitter.addListener(
      "set",
      () => {
        this.setState({ highlightedIndex: this.props.highlightState.index });
      }
    );

    // Subscribe to metadata changes to update filtered list after accept/ignore
    this.metadataSubscription = this.props.listState.emitter.addListener(
      "change",
      () => {
        // Force re-render when metadata changes
        this.forceUpdate();
      }
    );

    window.addEventListener("keydown", this.handleKeyDown);
  }

  componentWillUnmount() {
    this.highlightStateSubscription!.remove();
    this.metadataSubscription?.remove();
    window.removeEventListener("keydown", this.handleKeyDown);
  }

  private handleHover = (index?: number) => {
    this.setState({ highlightedIndex: index });
  };
}
