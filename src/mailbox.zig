// Copyright (c) 2024 g41797
// SPDX-License-Identifier: MIT

const std = @import("std");
const Mutex = std.Io.Mutex;
const Condition = std.Io.Condition;

pub fn MailBox(comptime Letter: type) type {
    return struct {
        const Self = @This();

        /// Envelope inside FIFO wrapping the actual letter.
        pub const Envelope = struct {
            prev: ?*Envelope = null,
            next: ?*Envelope = null,
            letter: Letter,
        };

        first: ?*Envelope = null,
        last: ?*Envelope = null,
        len: usize = 0,
        closed: bool = false,
        mutex: Mutex = .init,
        condition: Condition = .init,

        /// Append a new Envelope to the tail
        /// and wake-up waiting on receive threads.
        /// Arguments:
        ///     new_Envelope: Pointer to the new Envelope to append.
        /// If mailbox was closed - returns error.Closed
        pub fn send(mbox: *Self, io: std.Io, new_Envelope: *Envelope) error{ Closed, Canceled }!void {
            try mbox.mutex.lock(io);
            defer mbox.mutex.unlock(io);

            if (mbox.closed) {
                return error.Closed;
            }

            mbox.enqueue(new_Envelope);

            mbox.condition.signal(io);
        }

        /// Blocks thread until an envelope is available, FIFO.
        pub fn receive(mbox: *Self, io: std.Io, timeout: std.Io.Timeout) error{ Canceled, Timeout, Closed }!*Envelope {
            try mbox.mutex.lock(io);
            defer mbox.mutex.unlock(io);

            while (mbox.len == 0) {
                if (mbox.closed) {
                    return error.Closed;
                }

                try mbox.condition.waitTimeout(io, &mbox.mutex, timeout);
            }

            if (mbox.closed) {
                return error.Closed;
            }

            const first = mbox.dequeue();

            if (first) |firstEnvelope| {
                defer mbox.condition.signal(io);
                return firstEnvelope;
            } else {
                return error.Timeout;
            }
        }

        /// # of letters in internal queue.
        /// May be called also on closed mailbox.
        pub fn letters(mbox: *Self, io: std.Io) usize {
            try mbox.mutex.lock(io);
            defer mbox.mutex.unlock(io);

            return mbox.len;
        }

        /// First close disabled further client calls and returns head of Envelopes
        /// for de-allocation
        pub fn close(mbox: *Self, io: std.Io) ?*Envelope {
            mbox.mutex.lock();
            defer mbox.mutex.unlock();

            if (mbox.closed) return null;

            mbox.closed = true;

            const head = mbox.first;

            mbox.first = null;

            mbox.cond.signal(io);

            return head;
        }

        fn enqueue(fifo: *Self, new_Envelope: *Envelope) void {
            new_Envelope.prev = null;
            new_Envelope.next = null;

            if (fifo.last) |last| {
                last.next = new_Envelope;
                new_Envelope.prev = last;
            } else {
                fifo.first = new_Envelope;
            }

            fifo.last = new_Envelope;
            fifo.len += 1;

            return;
        }

        fn dequeue(fifo: *Self) ?*Envelope {
            if (fifo.len == 0) {
                return null;
            }

            var result = fifo.first;
            fifo.first = result.?.next;

            if (fifo.len == 1) {
                fifo.last = null;
            } else {
                fifo.first.?.prev = fifo.first;
            }

            result.?.prev = null;
            result.?.next = null;
            fifo.len -= 1;

            return result;
        }
    };
}
//-----------------------------
