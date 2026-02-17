/**
 * Plot Data Compression Utilities
 * Handles decompression of plot data transferred from server
 */

class PlotCompression {
    static async decompressData(compressedResult) {
        try {
            console.log('Decompression: Starting data decompression...');
            console.log('Compression method:', compressedResult.compression_method);
            console.log('Compressed size:', compressedResult.compressed_size, 'bytes');
            console.log('Original size:', compressedResult.original_size, 'bytes');
            
            // Handle uncompressed fallback
            if (compressedResult.compression_method === 'none') {
                console.log('Decompression: Using uncompressed fallback data');
                return compressedResult.uncompressed_data;
            }
            
            // Decode base64
            const compressedBytes = Uint8Array.from(atob(compressedResult.compressed_data), c => c.charCodeAt(0));
            console.log('Decompression: Base64 decoded to', compressedBytes.length, 'bytes');
            
            let decompressedBytes;
            
            // Decompress based on method
            if (compressedResult.compression_method === 'brotli') {
                // Try different ways to access Brotli decoder
                let brotliDecoder = null;
                if (typeof BrotliDecode !== 'undefined') {
                    brotliDecoder = BrotliDecode;
                } else if (typeof window.BrotliDecode !== 'undefined') {
                    brotliDecoder = window.BrotliDecode;
                } else if (typeof window.brotli !== 'undefined' && window.brotli.decode) {
                    brotliDecoder = window.brotli.decode;
                }
                
                if (!brotliDecoder) {
                    throw new Error('Brotli decoder not available - falling back to gzip');
                }
                
                console.log('Decompression: Using Brotli decompression...');
                decompressedBytes = brotliDecoder(compressedBytes);
            } else if (compressedResult.compression_method === 'gzip') {
                console.log('Decompression: Using gzip decompression...');
                // Use pako library for gzip decompression
                if (typeof pako === 'undefined') {
                    throw new Error('Pako (gzip) decoder not available');
                }
                decompressedBytes = pako.inflate(compressedBytes);
            } else {
                throw new Error(`Unsupported compression method: ${compressedResult.compression_method}`);
            }
            
            // Convert back to string and parse JSON
            const decompressedString = new TextDecoder().decode(decompressedBytes);
            const plotData = JSON.parse(decompressedString);
            
            console.log('Success: Decompression: Plot data decompressed successfully');
            console.log('Decompressed to', decompressedString.length, 'characters');
            
            return plotData;
            
        } catch (error) {
            console.error('Error: Decompression: Failed to decompress plot data:', error);
            throw error;
        }
    }
    
    static async loadCompressedPlotData(sessionId, compressionMethod = null) {
        try {
            console.log('Loading: Fetching compressed plot data for session:', sessionId);
            
            // Determine best compression method if not specified
            if (!compressionMethod) {
                const support = checkDecompressionSupport();
                if (support.gzip) {
                    compressionMethod = 'gzip'; // Prefer gzip for compatibility
                } else if (support.brotli) {
                    compressionMethod = 'brotli';
                } else {
                    console.warn('No compression support detected, requesting gzip anyway');
                    compressionMethod = 'gzip';
                }
            }
            
            const url = `/api/plot-data-compressed/${sessionId}?compression=${compressionMethod}`;
            console.log('Loading: Using compression method:', compressionMethod);
            
            const startTime = performance.now();
            const response = await fetch(url);
            const fetchTime = performance.now();
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const compressedResult = await response.json();
            const parseTime = performance.now();
            
            // Log compression stats from headers
            const originalSize = response.headers.get('X-Original-Size');
            const compressedSize = response.headers.get('X-Compressed-Size');
            const compressionRatio = response.headers.get('X-Compression-Ratio');
            
            console.log('Stats: Transfer completed:', {
                originalSize: originalSize ? `${(parseInt(originalSize) / 1024 / 1024).toFixed(2)} MB` : 'unknown',
                compressedSize: compressedSize ? `${(parseInt(compressedSize) / 1024 / 1024).toFixed(2)} MB` : 'unknown',
                compressionRatio: compressionRatio || 'unknown',
                fetchTime: `${(fetchTime - startTime).toFixed(1)}ms`,
                parseTime: `${(parseTime - fetchTime).toFixed(1)}ms`
            });
            
            // Decompress the data
            const decompressStartTime = performance.now();
            const plotData = await this.decompressData(compressedResult);
            const decompressEndTime = performance.now();
            
            console.log('Stats: Decompression completed in', `${(decompressEndTime - decompressStartTime).toFixed(1)}ms`);
            console.log('Stats: Total time (fetch + decompress):', `${(decompressEndTime - startTime).toFixed(1)}ms`);
            
            return plotData;
            
        } catch (error) {
            console.error('Error: Loading: Failed to load compressed plot data:', error);
            
            // Try fallback compression method if first attempt failed
            if (compressionMethod === 'brotli') {
                console.log('Loading: Retrying with gzip compression...');
                try {
                    return await this.loadCompressedPlotData(sessionId, 'gzip');
                } catch (fallbackError) {
                    console.error('Error: Loading: Fallback gzip also failed:', fallbackError);
                }
            }
            
            throw error;
        }
    }
}

// Fallback decompression libraries check
function checkDecompressionSupport() {
    const support = {
        brotli: (typeof BrotliDecode !== 'undefined') || (typeof window.BrotliDecode !== 'undefined'),
        gzip: typeof pako !== 'undefined'
    };
    
    console.log('Decompression Support:', support);
    
    if (!support.brotli && !support.gzip) {
        console.warn('Warning: No decompression libraries available. Install pako.js or brotli.js for compression support.');
    }
    
    return support;
}

// Wait for libraries to load before initializing
function waitForLibraries() {
    return new Promise((resolve) => {
        let attempts = 0;
        const maxAttempts = 50; // 5 seconds max wait
        
        const checkLibs = () => {
            const support = checkDecompressionSupport();
            if (support.gzip || support.brotli || attempts >= maxAttempts) {
                resolve(support);
            } else {
                attempts++;
                setTimeout(checkLibs, 100);
            }
        };
        
        checkLibs();
    });
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', async function() {
    console.log('Compression: Waiting for decompression libraries...');
    const support = await waitForLibraries();
    console.log('Compression: Library loading complete:', support);
});

// Export for global use
window.PlotCompression = PlotCompression;