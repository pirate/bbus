package bubus

import "sync"

type EventHistory struct {
	mu             sync.RWMutex
	events         map[string]*BaseEvent
	order          []string
	MaxHistorySize *int
	MaxHistoryDrop bool
}

func NewEventHistory(max_history_size *int, max_history_drop bool) *EventHistory {
	return &EventHistory{events: map[string]*BaseEvent{}, MaxHistorySize: max_history_size, MaxHistoryDrop: max_history_drop}
}

func (h *EventHistory) AddEvent(event *BaseEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.events[event.EventID]; !ok {
		h.order = append(h.order, event.EventID)
	}
	h.events[event.EventID] = event
}

func (h *EventHistory) GetEvent(event_id string) *BaseEvent {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.events[event_id]
}

func (h *EventHistory) Has(event_id string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.events[event_id]
	return ok
}

func (h *EventHistory) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.events)
}

func (h *EventHistory) Values() []*BaseEvent {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*BaseEvent, 0, len(h.order))
	for _, id := range h.order {
		if e := h.events[id]; e != nil {
			out = append(out, e)
		}
	}
	return out
}

func (h *EventHistory) RemoveEvent(event_id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.events[event_id]; !ok {
		return false
	}
	delete(h.events, event_id)
	for i := len(h.order) - 1; i >= 0; i-- {
		if h.order[i] == event_id {
			h.order = append(h.order[:i], h.order[i+1:]...)
			break
		}
	}
	return true
}

func (h *EventHistory) TrimEventHistory(is_event_complete func(event *BaseEvent) bool) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.MaxHistorySize == nil {
		return 0
	}
	max := *h.MaxHistorySize
	overage := len(h.events) - max
	if overage <= 0 {
		return 0
	}
	if is_event_complete == nil {
		is_event_complete = func(event *BaseEvent) bool { return event.EventStatus == "completed" }
	}
	removed := 0
	for overage > 0 {
		removed_any := false
		for i := 0; i < len(h.order) && overage > 0; i++ {
			eid := h.order[i]
			e := h.events[eid]
			if e == nil || !is_event_complete(e) {
				continue
			}
			delete(h.events, eid)
			h.order = append(h.order[:i], h.order[i+1:]...)
			i--
			overage--
			removed++
			removed_any = true
		}
		if removed_any {
			continue
		}
		if len(h.order) == 0 {
			break
		}
		eid := h.order[0]
		delete(h.events, eid)
		h.order = h.order[1:]
		overage--
		removed++
	}
	return removed
}
