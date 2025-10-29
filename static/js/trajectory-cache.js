/**
 * TrajectoryCache - Browser-based caching system for trajectory files
 * Uses IndexedDB for persistent storage across browser sessions
 */
class TrajectoryCache {
    constructor() {
        this.dbName = 'TrajectoryCache';
        this.version = 1;
        this.storeName = 'trajectoryFiles';
        this.db = null;
        this.initialized = false;
        
        // Initialize the database
        this.init();
    }

    async init() {
        try {
            // Check if IndexedDB is supported
            if (!window.indexedDB) {
                console.warn('IndexedDB not supported, cache will not work');
                return;
            }

            // Open the database
            this.db = await this.openDB();
            this.initialized = true;
            console.log('TrajectoryCache initialized successfully');
        } catch (error) {
            console.error('Failed to initialize TrajectoryCache:', error);
        }
    }

    openDB() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, this.version);

            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);

            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains(this.storeName)) {
                    const store = db.createObjectStore(this.storeName, { keyPath: 'key' });
                    store.createIndex('sessionId', 'sessionId', { unique: false });
                    store.createIndex('fileName', 'fileName', { unique: false });
                    store.createIndex('timestamp', 'timestamp', { unique: false });
                }
            };
        });
    }

    generateKey(sessionId, fileName) {
        return `${sessionId}_${fileName}`;
    }

    async getFile(sessionId, fileName) {
        if (!this.initialized || !this.db) {
            return null;
        }

        try {
            const key = this.generateKey(sessionId, fileName);
            const transaction = this.db.transaction([this.storeName], 'readonly');
            const store = transaction.objectStore(this.storeName);
            const request = store.get(key);

            return new Promise((resolve, reject) => {
                request.onsuccess = () => {
                    const result = request.result;
                    if (result) {
                        console.log(`Cache hit: ${fileName} (${(result.size / 1024 / 1024).toFixed(2)}MB)`);
                        resolve(result.data);
                    } else {
                        resolve(null);
                    }
                };
                request.onerror = () => reject(request.error);
            });
        } catch (error) {
            console.error('Error getting file from cache:', error);
            return null;
        }
    }

    async storeFile(sessionId, fileName, data, contentType = 'application/octet-stream') {
        if (!this.initialized || !this.db) {
            return false;
        }

        try {
            const key = this.generateKey(sessionId, fileName);
            const fileData = {
                key: key,
                sessionId: sessionId,
                fileName: fileName,
                data: data,
                contentType: contentType,
                size: data.byteLength || data.length || 0,
                timestamp: Date.now()
            };

            const transaction = this.db.transaction([this.storeName], 'readwrite');
            const store = transaction.objectStore(this.storeName);
            const request = store.put(fileData);

            return new Promise((resolve, reject) => {
                request.onsuccess = () => {
                    console.log(`Cached: ${fileName} (${(fileData.size / 1024 / 1024).toFixed(2)}MB)`);
                    resolve(true);
                };
                request.onerror = () => reject(request.error);
            });
        } catch (error) {
            console.error('Error storing file in cache:', error);
            return false;
        }
    }

    async downloadAndCache(url, sessionId, fileName) {
        try {
            console.log(`Downloading and caching: ${fileName} from ${url}`);
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.arrayBuffer();
            const contentType = response.headers.get('content-type') || 'application/octet-stream';

            // Store in cache
            await this.storeFile(sessionId, fileName, data, contentType);

            // Return processed format that matches what the UI expects
            const blob = new Blob([data], { type: contentType });
            const blobUrl = URL.createObjectURL(blob);
            
            return {
                url: blobUrl,
                fromCache: false, // Downloaded fresh, then cached
                fallback: false,
                size: data.byteLength,
                data: data // Keep raw data available if needed
            };
        } catch (error) {
            console.error('Error downloading and caching file:', error);
            // Return fallback format
            return {
                url: url, // Fallback to original server URL
                fromCache: false,
                fallback: true,
                size: 0,
                data: null
            };
        }
    }

    async getCacheStats() {
        if (!this.initialized || !this.db) {
            return { supported: false, count: 0, totalSizeMB: 0 };
        }

        try {
            const transaction = this.db.transaction([this.storeName], 'readonly');
            const store = transaction.objectStore(this.storeName);
            const request = store.getAll();

            return new Promise((resolve, reject) => {
                request.onsuccess = () => {
                    const entries = request.result;
                    const totalSize = entries.reduce((sum, entry) => sum + (entry.size || 0), 0);
                    
                    const stats = {
                        supported: true,
                        count: entries.length,
                        totalSizeMB: Math.round(totalSize / 1024 / 1024 * 100) / 100,
                        entries: entries.map(e => ({
                            fileName: e.fileName,
                            sessionId: e.sessionId,
                            sizeMB: Math.round(e.size / 1024 / 1024 * 100) / 100,
                            timestamp: new Date(e.timestamp).toLocaleString()
                        })),
                        structureCount: entries.filter(e => e.fileName.endsWith('.pdb')).length,
                        trajectoryCount: entries.filter(e => e.fileName.endsWith('.xtc')).length,
                        utilizationPercent: Math.min(100, Math.round(totalSize / (100 * 1024 * 1024) * 100))
                    };

                    resolve(stats);
                };
                request.onerror = () => reject(request.error);
            });
        } catch (error) {
            console.error('Error getting cache stats:', error);
            return { supported: false, count: 0, totalSizeMB: 0 };
        }
    }

    async clearCache(sessionId = null) {
        if (!this.initialized || !this.db) {
            return false;
        }

        try {
            const transaction = this.db.transaction([this.storeName], 'readwrite');
            const store = transaction.objectStore(this.storeName);

            if (sessionId) {
                // Clear only specific session
                const index = store.index('sessionId');
                const request = index.getAllKeys(sessionId);
                
                return new Promise((resolve, reject) => {
                    request.onsuccess = () => {
                        const keys = request.result;
                        let deleted = 0;
                        
                        keys.forEach(key => {
                            store.delete(key).onsuccess = () => {
                                deleted++;
                                if (deleted === keys.length) {
                                    console.log(`Cleared ${deleted} files for session ${sessionId}`);
                                    resolve(true);
                                }
                            };
                        });
                        
                        if (keys.length === 0) resolve(true);
                    };
                    request.onerror = () => reject(request.error);
                });
            } else {
                // Clear all cache
                const request = store.clear();
                return new Promise((resolve, reject) => {
                    request.onsuccess = () => {
                        console.log('Cache cleared completely');
                        resolve(true);
                    };
                    request.onerror = () => reject(request.error);
                });
            }
        } catch (error) {
            console.error('Error clearing cache:', error);
            return false;
        }
    }

    async deleteFile(sessionId, fileName) {
        if (!this.initialized || !this.db) {
            return false;
        }

        try {
            const key = this.generateKey(sessionId, fileName);
            const transaction = this.db.transaction([this.storeName], 'readwrite');
            const store = transaction.objectStore(this.storeName);
            const request = store.delete(key);

            return new Promise((resolve, reject) => {
                request.onsuccess = () => {
                    console.log(`Deleted from cache: ${fileName}`);
                    resolve(true);
                };
                request.onerror = () => reject(request.error);
            });
        } catch (error) {
            console.error('Error deleting file from cache:', error);
            return false;
        }
    }
}

// Initialize global trajectory cache instance
window.trajectoryCache = new TrajectoryCache();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = TrajectoryCache;
}