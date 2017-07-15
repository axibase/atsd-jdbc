package com.axibase.tsd.driver.jdbc.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class WildcardsUtil {
	private static final char ONE_ANY_SYMBOL = '_';
	private static final char NONE_OR_MORE_SYMBOLS = '%';
	private static final char ATSD_WILDCARD = '*';

	public static boolean hasWildcards(String text) {
		return text == null || text.indexOf(ONE_ANY_SYMBOL) != -1 || text.indexOf(NONE_OR_MORE_SYMBOLS) != -1;
	}

	public static boolean wildcardMatch(String text, String pattern) {
		return wildcardMatch(text, pattern, ONE_ANY_SYMBOL, NONE_OR_MORE_SYMBOLS);
	}

	public static boolean atsdWildcardMatch(String text, String pattern) {
		return wildcardMatch(text, pattern, '\0', ATSD_WILDCARD);
	}

	private static boolean wildcardMatch(String text, String pattern, char anySymbol, char manySymbol) {
		if (pattern == null) {
			return true;
		}
		if (text == null) {
			return false;
		}

		final String oneAnySymbolStr = String.valueOf(anySymbol);
		final String noneOrMoreSymbolsStr = String.valueOf(manySymbol);
		final int stringLength = text.length();
		final String[] wcs = splitOnTokens(pattern, oneAnySymbolStr, noneOrMoreSymbolsStr);
		boolean anyChars = false;
		int textIdx = 0;
		int wcsIdx = 0;
		Stack<int[]> backtrack = new Stack<>();

		// loop around a backtrack stack to handle complex % matching
		do {
			if (backtrack.size() > 0) {
				int[] array = backtrack.pop();
				wcsIdx = array[0];
				textIdx = array[1];
				anyChars = true;
			}

			// loop whilst tokens and text left to process
			while (wcsIdx < wcs.length) {
				if (oneAnySymbolStr.equals(wcs[wcsIdx])) {
					// found _, hence move to next text char
					textIdx++;
					if (textIdx > stringLength) {
						break;
					}
					anyChars = false;

				} else if (noneOrMoreSymbolsStr.equals(wcs[wcsIdx])) {
					anyChars = true;
					if (wcsIdx == wcs.length - 1) {
						textIdx = stringLength;
					}

				} else {
					if (anyChars) {
						// any chars, hence try to locate text token
						textIdx = StringUtils.indexOfIgnoreCase(text, wcs[wcsIdx]);
						if (textIdx == -1) {
							break;
						}
						int repeat = StringUtils.indexOfIgnoreCase(text, wcs[wcsIdx], textIdx  +1);
						if (repeat >= 0) {
							backtrack.push(new int[] {wcsIdx, repeat});
						}
					} else {
						// matching from current position
						if (!text.regionMatches(true, textIdx, wcs[wcsIdx], 0, wcs[wcsIdx].length())) {
							// couldn't match token
							break;
						}
					}

					// matched text token, move text index to end of matched token
					textIdx += wcs[wcsIdx].length();
					anyChars = false;
				}

				wcsIdx++;
			}

			// full match
			if (wcsIdx == wcs.length && textIdx == text.length()) {
				return true;
			}

		} while (backtrack.size() > 0);

		return false;
	}

	/**
	 * Splits a string into a number of tokens.
	 * The text is split by '_' and '%'.
	 * Multiple '%' are  are collapsed into a single '%'.
	 *
	 * @param text  the text to split
	 * @return the array of tokens, never null
	 */
	private static String[] splitOnTokens(String text, String oneAnySymbolStr, String noneOrMoreSymbolsStr) {
		if (!hasWildcards(text)) {
			return new String[] { text };
		}
		List<String> list = new ArrayList<>();
		StringBuilder buffer = new StringBuilder();

		final int length = text.length();
		for (int i = 0; i < length; i++) {
			final char current = text.charAt(i);

			switch (current) {
				case ONE_ANY_SYMBOL:
					flushBuffer(buffer, list);
					if (!list.isEmpty() && noneOrMoreSymbolsStr.equals(list.get(list.size() - 1))) {
						list.set(list.size() - 1, oneAnySymbolStr);
						list.add(noneOrMoreSymbolsStr);
					} else {
						list.add(oneAnySymbolStr);
					}
					break;
				case NONE_OR_MORE_SYMBOLS:
					flushBuffer(buffer, list);
					if (list.isEmpty() || i > 0 && !noneOrMoreSymbolsStr.equals(list.get(list.size() - 1))) {
						list.add(noneOrMoreSymbolsStr);
					}
					break;
				default:
					buffer.append(current);
			}
		}
		if (buffer.length() != 0) {
			list.add(buffer.toString());
		}

		return list.toArray(new String[list.size()]);
	}

	private static void flushBuffer(StringBuilder buffer, List<String> list) {
		if (buffer.length() != 0) {
			list.add(buffer.toString());
			buffer.setLength(0);
		}
	}
}
