/**
 * DOM Utilities - DOM Manipulation and Helper Functions
 * 
 * Handles:
 * - Element creation and manipulation
 * - Event handling utilities
 * - Style and class management
 * - Form and input utilities
 * - Accessibility helpers
 * 
 * Extracted from the monolithic election_map.html JavaScript code.
 */

/**
 * Create element with attributes and content
 */
export function createElement(tag, attributes = {}, content = '') {
    const element = document.createElement(tag);
    
    // Set attributes
    Object.keys(attributes).forEach(key => {
        if (key === 'className') {
            element.className = attributes[key];
        } else if (key === 'style' && typeof attributes[key] === 'object') {
            Object.assign(element.style, attributes[key]);
        } else {
            element.setAttribute(key, attributes[key]);
        }
    });
    
    // Set content
    if (content) {
        if (typeof content === 'string') {
            element.innerHTML = content;
        } else if (content instanceof Node) {
            element.appendChild(content);
        } else if (Array.isArray(content)) {
            content.forEach(child => {
                if (child instanceof Node) {
                    element.appendChild(child);
                } else {
                    element.appendChild(document.createTextNode(child));
                }
            });
        }
    }
    
    return element;
}

/**
 * Get element by ID with error handling
 */
export function getElement(id) {
    const element = document.getElementById(id);
    if (!element) {
        console.warn(`[DOMUtils] Element with ID '${id}' not found`);
    }
    return element;
}

/**
 * Get elements by selector with error handling
 */
export function getElements(selector) {
    try {
        return document.querySelectorAll(selector);
    } catch (error) {
        console.warn(`[DOMUtils] Invalid selector '${selector}':`, error);
        return [];
    }
}

/**
 * Show/hide element
 */
export function setElementVisibility(element, visible) {
    if (!element) return;
    
    element.style.display = visible ? '' : 'none';
}

/**
 * Toggle element visibility
 */
export function toggleElementVisibility(element) {
    if (!element) return false;
    
    const isVisible = element.style.display !== 'none';
    setElementVisibility(element, !isVisible);
    return !isVisible;
}

/**
 * Add class to element
 */
export function addClass(element, className) {
    if (element && className) {
        element.classList.add(className);
    }
}

/**
 * Remove class from element
 */
export function removeClass(element, className) {
    if (element && className) {
        element.classList.remove(className);
    }
}

/**
 * Toggle class on element
 */
export function toggleClass(element, className) {
    if (element && className) {
        return element.classList.toggle(className);
    }
    return false;
}

/**
 * Check if element has class
 */
export function hasClass(element, className) {
    return element && className && element.classList.contains(className);
}

/**
 * Set multiple attributes on element
 */
export function setAttributes(element, attributes) {
    if (!element || !attributes) return;
    
    Object.keys(attributes).forEach(key => {
        element.setAttribute(key, attributes[key]);
    });
}

/**
 * Remove attribute from element
 */
export function removeAttribute(element, attribute) {
    if (element && attribute) {
        element.removeAttribute(attribute);
    }
}

/**
 * Set element text content safely
 */
export function setTextContent(element, text) {
    if (element) {
        element.textContent = text || '';
    }
}

/**
 * Set element HTML content safely
 */
export function setHTMLContent(element, html) {
    if (element) {
        element.innerHTML = html || '';
    }
}

/**
 * Clear element content
 */
export function clearElement(element) {
    if (element) {
        element.innerHTML = '';
    }
}

/**
 * Remove element from DOM
 */
export function removeElement(element) {
    if (element && element.parentNode) {
        element.parentNode.removeChild(element);
    }
}

/**
 * Get element's computed style property
 */
export function getComputedStyleProperty(element, property) {
    if (!element) return null;
    
    const styles = window.getComputedStyle(element);
    return styles.getPropertyValue(property);
}

/**
 * Check if element is visible
 */
export function isElementVisible(element) {
    if (!element) return false;
    
    const style = window.getComputedStyle(element);
    return style.display !== 'none' && 
           style.visibility !== 'hidden' && 
           style.opacity !== '0';
}

/**
 * Get element dimensions
 */
export function getElementDimensions(element) {
    if (!element) return { width: 0, height: 0 };
    
    const rect = element.getBoundingClientRect();
    return {
        width: rect.width,
        height: rect.height,
        top: rect.top,
        left: rect.left,
        bottom: rect.bottom,
        right: rect.right
    };
}

/**
 * Scroll element into view smoothly
 */
export function scrollIntoView(element, options = {}) {
    if (!element) return;
    
    const defaultOptions = {
        behavior: 'smooth',
        block: 'center',
        inline: 'nearest'
    };
    
    element.scrollIntoView({ ...defaultOptions, ...options });
}

/**
 * Add event listener with cleanup tracking
 */
export function addEventListener(element, event, handler, options = {}) {
    if (!element || !event || !handler) return null;
    
    element.addEventListener(event, handler, options);
    
    // Return cleanup function
    return () => {
        element.removeEventListener(event, handler, options);
    };
}

/**
 * Add multiple event listeners
 */
export function addEventListeners(element, events) {
    if (!element || !events) return [];
    
    const cleanupFunctions = [];
    
    Object.keys(events).forEach(event => {
        const cleanup = addEventListener(element, event, events[event]);
        if (cleanup) {
            cleanupFunctions.push(cleanup);
        }
    });
    
    return cleanupFunctions;
}

/**
 * Delegate event handling
 */
export function delegateEvent(container, selector, event, handler) {
    if (!container || !selector || !event || !handler) return null;
    
    const delegateHandler = (e) => {
        const target = e.target.closest(selector);
        if (target && container.contains(target)) {
            handler.call(target, e);
        }
    };
    
    return addEventListener(container, event, delegateHandler);
}

/**
 * Create select option element
 */
export function createOption(value, text, selected = false) {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = text;
    option.selected = selected;
    return option;
}

/**
 * Populate select element with options
 */
export function populateSelect(selectElement, options, clearFirst = true) {
    if (!selectElement) return;
    
    if (clearFirst) {
        clearElement(selectElement);
    }
    
    options.forEach(({ value, text, selected }) => {
        const option = createOption(value, text, selected);
        selectElement.appendChild(option);
    });
}

/**
 * Get form data as object
 */
export function getFormData(form) {
    if (!form) return {};
    
    const formData = new FormData(form);
    const data = {};
    
    for (const [key, value] of formData.entries()) {
        data[key] = value;
    }
    
    return data;
}

/**
 * Set form field value
 */
export function setFormFieldValue(form, fieldName, value) {
    if (!form || !fieldName) return;
    
    const field = form.querySelector(`[name="${fieldName}"]`);
    if (field) {
        if (field.type === 'checkbox' || field.type === 'radio') {
            field.checked = Boolean(value);
        } else {
            field.value = value;
        }
    }
}

/**
 * Add ARIA attributes for accessibility
 */
export function addAriaAttributes(element, attributes) {
    if (!element || !attributes) return;
    
    Object.keys(attributes).forEach(key => {
        const ariaKey = key.startsWith('aria-') ? key : `aria-${key}`;
        element.setAttribute(ariaKey, attributes[key]);
    });
}

/**
 * Create loading element
 */
export function createLoadingElement(text = 'Loading...') {
    return createElement('div', {
        className: 'loading-indicator',
        style: {
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: '1rem',
            color: 'var(--color-text-secondary)',
            fontStyle: 'italic'
        }
    }, text);
}

/**
 * Create error element
 */
export function createErrorElement(message = 'An error occurred') {
    return createElement('div', {
        className: 'error-message',
        style: {
            padding: '1rem',
            background: '#fee2e2',
            color: '#dc2626',
            border: '1px solid #fca5a5',
            borderRadius: 'var(--border-radius)'
        }
    }, message);
}

/**
 * Debounce function calls
 */
export function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Throttle function calls
 */
export function throttle(func, limit) {
    let inThrottle;
    return function executedFunction(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

/**
 * Copy text to clipboard
 */
export async function copyToClipboard(text) {
    if (navigator.clipboard && window.isSecureContext) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (error) {
            console.warn('[DOMUtils] Clipboard API failed:', error);
        }
    }
    
    // Fallback for older browsers
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    textArea.style.top = '-999999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        const successful = document.execCommand('copy');
        document.body.removeChild(textArea);
        return successful;
    } catch (error) {
        console.warn('[DOMUtils] Fallback copy failed:', error);
        document.body.removeChild(textArea);
        return false;
    }
}

/**
 * DOM utilities object for backward compatibility
 */
export const DOMUtils = {
    createElement,
    getElement,
    getElements,
    setElementVisibility,
    toggleElementVisibility,
    addClass,
    removeClass,
    toggleClass,
    hasClass,
    setAttributes,
    removeAttribute,
    setTextContent,
    setHTMLContent,
    clearElement,
    removeElement,
    getComputedStyleProperty,
    isElementVisible,
    getElementDimensions,
    scrollIntoView,
    addEventListener,
    addEventListeners,
    delegateEvent,
    createOption,
    populateSelect,
    getFormData,
    setFormFieldValue,
    addAriaAttributes,
    createLoadingElement,
    createErrorElement,
    debounce,
    throttle,
    copyToClipboard
}; 