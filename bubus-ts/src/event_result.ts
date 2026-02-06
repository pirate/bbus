import { v7 as uuidv7 } from "uuid";

import type { BaseEvent } from "./base_event.js";

export type EventResultStatus = "pending" | "started" | "completed" | "error";

export class EventResult {
  id: string;
  status: EventResultStatus;
  event_id: string;
  handler_id: string;
  handler_name: string;
  handler_file_path?: string;
  eventbus_name: string;
  started_at?: string;
  completed_at?: string;
  result?: unknown;
  error?: unknown;
  event_children: BaseEvent[];
  queue_jump_hold: boolean;

  constructor(params: {
    event_id: string;
    handler_id: string;
    handler_name: string;
    handler_file_path?: string;
    eventbus_name: string;
  }) {
    this.id = uuidv7();
    this.status = "pending";
    this.event_id = params.event_id;
    this.handler_id = params.handler_id;
    this.handler_name = params.handler_name;
    this.handler_file_path = params.handler_file_path;
    this.eventbus_name = params.eventbus_name;
    this.event_children = [];
    this.queue_jump_hold = false;
  }

  markStarted(): void {
    this.status = "started";
    this.started_at = new Date().toISOString();
  }

  markCompleted(result: unknown): void {
    this.status = "completed";
    this.result = result;
    this.completed_at = new Date().toISOString();
  }

  markError(error: unknown): void {
    this.status = "error";
    this.error = error;
    this.completed_at = new Date().toISOString();
  }
}
