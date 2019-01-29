"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class CancelToken {
    constructor() {
        this.cancelled = false;
    }
    isCancelled() {
        return this.cancelled;
    }
    cancel() {
        this.cancelled = true;
    }
}
exports.CancelToken = CancelToken;
//# sourceMappingURL=cancelToken.js.map